// 导入所需的标准库和第三方库
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;

// 错误处理和数据库相关的导入
use anyhow::{anyhow, bail, Context, Result};
use bili_sync_entity::*;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{Future, Stream, StreamExt, TryStreamExt};
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;
use sea_orm::TransactionTrait;
use tokio::fs;
use tokio::sync::Semaphore;

// 导入本地模块
use crate::adapter::{video_list_from, Args, VideoListModel, VideoListModelEnum};
use crate::bilibili::{BestStream, BiliClient, BiliError, Dimension, PageInfo, Video, VideoInfo};
use crate::config::{PathSafeTemplate, ARGS, CONFIG, TEMPLATE};
use crate::downloader::Downloader;
use crate::error::{DownloadAbortError, ProcessPageError};
use crate::utils::format_arg::{page_format_args, video_format_args};
use crate::utils::model::{
    create_pages, create_videos, filter_unfilled_videos, filter_unhandled_video_pages, update_pages_model,
    update_videos_model,
};
use crate::utils::nfo::{ModelWrapper, NFOMode, NFOSerializer};
use crate::utils::status::{PageStatus, VideoStatus};

/// 完整地处理某个视频列表
/// 
/// 主要流程:
/// 1. 获取视频列表信息
/// 2. 刷新视频列表,获取新视频
/// 3. 获取视频详细信息
/// 4. 下载视频内容(如果不是仅扫描模式)
pub async fn process_video_list(
    args: Args<'_>,
    bili_client: &BiliClient,
    path: &Path,
    connection: &DatabaseConnection,
) -> Result<()> {
    // 从参数中获取视频列表的 Model 与视频流
    let (video_list_model, video_streams) = video_list_from(args, path, bili_client, connection).await?;
    // 从视频流中获取新视频的简要信息，写入数据库
    refresh_video_list(&video_list_model, video_streams, connection).await?;
    // 单独请求视频详情接口，获取视频的详情信息与所有的分页，写入数据库
    fetch_video_details(bili_client, &video_list_model, connection).await?;
    if ARGS.scan_only {
        warn!("已开启仅扫描模式，跳过视频下载..");
    } else {
        // 从数据库中查找所有未下载的视频与分页，下载并处理
        download_unprocessed_videos(bili_client, &video_list_model, connection).await?;
    }
    Ok(())
}

/// 请求接口，获取视频列表中所有新添加的视频信息，将其写入数据库
///
/// 工作流程:
/// 1. 获取最新视频的时间戳
/// 2. 遍历视频流,过滤出新视频
/// 3. 批量写入数据库
/// 4. 更新最新视频时间戳
pub async fn refresh_video_list<'a>(
    video_list_model: &VideoListModelEnum,
    video_streams: Pin<Box<dyn Stream<Item = Result<VideoInfo>> + 'a + Send>>,
    connection: &DatabaseConnection,
) -> Result<()> {
    video_list_model.log_refresh_video_start();
    let latest_row_at = video_list_model.get_latest_row_at().and_utc();
    let mut max_datetime = latest_row_at;
    let mut error = Ok(());
    let mut video_streams = video_streams
        .take_while(|res| {
            match res {
                Err(e) => {
                    error = Err(anyhow!(e.to_string()));
                    futures::future::ready(false)
                }
                Ok(v) => {
                    // 虽然 video_streams 是从新到旧的，但由于此处是分页请求，极端情况下可能发生访问完第一页时插入了两整页视频的情况
                    // 此时获取到的第二页视频比第一页的还要新，因此为了确保正确，理应对每一页的第一个视频进行时间比较
                    // 但在 streams 的抽象下，无法判断具体是在哪里分页的，所以暂且对每个视频都进行比较，应该不会有太大性能损失
                    let release_datetime = v.release_datetime();
                    if release_datetime > &max_datetime {
                        max_datetime = *release_datetime;
                    }
                    futures::future::ready(release_datetime > &latest_row_at)
                }
            }
        })
        .filter_map(|res| futures::future::ready(res.ok()))
        .chunks(10);
    let mut count = 0;
    while let Some(videos_info) = video_streams.next().await {
        count += videos_info.len();
        create_videos(videos_info, video_list_model, connection).await?;
    }
    // 如果获取视频分页过程中发生了错误，直接在此处返回，不更新 latest_row_at
    error?;
    if max_datetime != latest_row_at {
        video_list_model
            .update_latest_row_at(max_datetime.naive_utc())
            .save(connection)
            .await?;
    }
    video_list_model.log_refresh_video_end(count);
    Ok(())
}

/// 筛选出所有未获取到全部信息的视频，尝试补充其详细信息
///
/// 工作流程:
/// 1. 获取未填充完整的视频列表
/// 2. 遍历视频列表,获取每个视频的标签和详情
/// 3. 处理404等错误情况
/// 4. 保存视频详情到数据库
pub async fn fetch_video_details(
    bili_client: &BiliClient,
    video_list_model: &VideoListModelEnum,
    connection: &DatabaseConnection,
) -> Result<()> {
    video_list_model.log_fetch_video_start();
    let videos_model = filter_unfilled_videos(video_list_model.filter_expr(), connection).await?;
    for video_model in videos_model {
        let video = Video::new(bili_client, video_model.bvid.clone());
        let info: Result<_> = async { Ok((video.get_tags().await?, video.get_view_info().await?)) }.await;
        match info {
            Err(e) => {
                error!(
                    "获取视频 {} - {} 的详细信息失败，错误为：{}",
                    &video_model.bvid, &video_model.name, e
                );
                if let Some(BiliError::RequestFailed(-404, _)) = e.downcast_ref::<BiliError>() {
                    let mut video_active_model: bili_sync_entity::video::ActiveModel = video_model.into();
                    video_active_model.valid = Set(false);
                    video_active_model.save(connection).await?;
                }
            }
            Ok((tags, mut view_info)) => {
                let VideoInfo::Detail { pages, .. } = &mut view_info else {
                    unreachable!()
                };
                let pages = std::mem::take(pages);
                let pages_len = pages.len();
                let txn = connection.begin().await?;
                // 将分页信息写入数据库
                create_pages(pages, &video_model, &txn).await?;
                let mut video_active_model = view_info.into_detail_model(video_model);
                video_list_model.set_relation_id(&mut video_active_model);
                video_active_model.single_page = Set(Some(pages_len == 1));
                video_active_model.tags = Set(Some(serde_json::to_value(tags)?));
                video_active_model.save(&txn).await?;
                txn.commit().await?;
            }
        };
    }
    video_list_model.log_fetch_video_end();
    Ok(())
}

/// 下载所有未处理成功的视频
///
/// 工作流程:
/// 1. 创建并发控制信号量
/// 2. 获取未处理的视频列表
/// 3. 并发下载视频
/// 4. 处理风控和错误情况
/// 5. 批量更新数据库
pub async fn download_unprocessed_videos(
    bili_client: &BiliClient,
    video_list_model: &VideoListModelEnum,
    connection: &DatabaseConnection,
) -> Result<()> {
    video_list_model.log_download_video_start();
    let semaphore = Semaphore::new(CONFIG.concurrent_limit.video);
    let downloader = Downloader::new(bili_client.client.clone());
    let unhandled_videos_pages = filter_unhandled_video_pages(video_list_model.filter_expr(), connection).await?;
    let mut assigned_upper = HashSet::new();
    let tasks = unhandled_videos_pages
        .into_iter()
        .map(|(video_model, pages_model)| {
            let should_download_upper = !assigned_upper.contains(&video_model.upper_id);
            assigned_upper.insert(video_model.upper_id);
            // 并发下载视频
            download_video_pages(
                bili_client,
                video_list_model,
                video_model,
                pages_model,
                connection,
                &semaphore,
                &downloader,
                should_download_upper,
            )
        })
        .collect::<FuturesUnordered<_>>();
    let mut download_aborted = false;
    let mut stream = tasks
        // 触发风控时设置 download_aborted 标记并终止流
        .take_while(|res| {
            if res
                .as_ref()
                .is_err_and(|e| e.downcast_ref::<DownloadAbortError>().is_some())
            {
                download_aborted = true;
            }
            futures::future::ready(!download_aborted)
        })
        // 过滤掉没有触发风控的普通 Err，只保留正确返回的 Model
        .filter_map(|res| futures::future::ready(res.ok()))
        // 将成功返回的 Model 按十个一组合并
        .chunks(10);
    while let Some(models) = stream.next().await {
        update_videos_model(models, connection).await?;
    }
    if download_aborted {
        error!("下载触发风控，已终止所有任务，等待下一轮执行");
    }
    video_list_model.log_download_video_end();
    Ok(())
}

/// 下载单个视频的所有分页内容
///
/// 工作流程:
/// 1. 获取并发控制许可
/// 2. 获取视频状态
/// 3. 创建文件路径
/// 4. 并发下载视频相关资源:
///    - 视频封面
///    - 视频信息NFO
///    - UP主头像
///    - UP主信息NFO
///    - 视频分P内容
/// 5. 处理下载结果
/// 6. 更新视频状态
#[allow(clippy::too_many_arguments)]
pub async fn download_video_pages(
    bili_client: &BiliClient,
    video_list_model: &VideoListModelEnum,
    video_model: video::Model,
    pages: Vec<page::Model>,
    connection: &DatabaseConnection,
    semaphore: &Semaphore,
    downloader: &Downloader,
    should_download_upper: bool,
) -> Result<video::ActiveModel> {
    let _permit = semaphore.acquire().await.context("acquire semaphore failed")?;
    let mut status = VideoStatus::from(video_model.download_status);
    let separate_status = status.should_run();
    let base_path = video_list_model
        .path()
        .join(TEMPLATE.path_safe_render("video", &video_format_args(&video_model))?);
    let upper_id = video_model.upper_id.to_string();
    let base_upper_path = &CONFIG
        .upper_path
        .join(upper_id.chars().next().context("upper_id is empty")?.to_string())
        .join(upper_id);
    let is_single_page = video_model.single_page.context("single_page is null")?;
    // 对于单页视频，page 的下载已经足够
    // 对于多页视频，page 下载仅包含了分集内容，需要额外补上视频的 poster 的 tvshow.nfo
    let tasks: Vec<Pin<Box<dyn Future<Output = Result<()>> + Send>>> = vec![
        // 下载视频封面
        Box::pin(fetch_video_poster(
            separate_status[0] && !is_single_page,
            &video_model,
            downloader,
            base_path.join("poster.jpg"),
            base_path.join("fanart.jpg"),
        )),
        // 生成视频信息的 nfo
        Box::pin(generate_video_nfo(
            separate_status[1] && !is_single_page,
            &video_model,
            base_path.join("tvshow.nfo"),
        )),
        // 下载 Up 主头像
        Box::pin(fetch_upper_face(
            separate_status[2] && should_download_upper,
            &video_model,
            downloader,
            base_upper_path.join("folder.jpg"),
        )),
        // 生成 Up 主信息的 nfo
        Box::pin(generate_upper_nfo(
            separate_status[3] && should_download_upper,
            &video_model,
            base_upper_path.join("person.nfo"),
        )),
        // 分发并执行分 P 下载的任务
        Box::pin(dispatch_download_page(
            separate_status[4],
            bili_client,
            &video_model,
            pages,
            connection,
            downloader,
            &base_path,
        )),
    ];
    let tasks: FuturesOrdered<_> = tasks.into_iter().collect();
    let results: Vec<Result<()>> = tasks.collect().await;
    status.update_status(&results);
    results
        .iter()
        .take(4)
        .zip(["封面", "详情", "作者头像", "作者详情"])
        .for_each(|(res, task_name)| match res {
            Ok(_) => info!("处理视频「{}」{}成功", &video_model.name, task_name),
            Err(e) => error!("处理视频「{}」{}失败: {}", &video_model.name, task_name, e),
        });
    if let Err(e) = results.into_iter().nth(4).context("page download result not found")? {
        if e.downcast_ref::<DownloadAbortError>().is_some() {
            return Err(e);
        }
    }
    let mut video_active_model: video::ActiveModel = video_model.into();
    video_active_model.download_status = Set(status.into());
    video_active_model.path = Set(base_path.to_string_lossy().to_string());
    Ok(video_active_model)
}

/// 分发并执行分页下载任务，当且仅当所有分页成功下载或达到最大重试次数时返回 Ok，否则根据失败原因返回对应的错误
///
/// 工作流程:
/// 1. 检查是否需要执行
/// 2. 创建子任务并发控制
/// 3. 并发下载所有分页
/// 4. 处理下载结果
/// 5. 批量更新数据库
pub async fn dispatch_download_page(
    should_run: bool,
    bili_client: &BiliClient,
    video_model: &video::Model,
    pages: Vec<page::Model>,
    connection: &DatabaseConnection,
    downloader: &Downloader,
    base_path: &Path,
) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    let child_semaphore = Semaphore::new(CONFIG.concurrent_limit.page);
    let tasks = pages
        .into_iter()
        .map(|page_model| {
            download_page(
                bili_client,
                video_model,
                page_model,
                &child_semaphore,
                downloader,
                base_path,
            )
        })
        .collect::<FuturesUnordered<_>>();
    let (mut download_aborted, mut error_occurred) = (false, false);
    let mut stream = tasks
        .take_while(|res| {
            match res {
                Ok(model) => {
                    // 当前函数返回的是所有分页的下载状态，只要有任何一个分页返回新的下载状态标识位是 false，当前函数就应该认为是失败的
                    if model
                        .download_status
                        .try_as_ref()
                        .is_none_or(|status| !PageStatus::from(*status).get_completed())
                    {
                        error_occurred = true;
                    }
                }
                Err(e) => {
                    if e.downcast_ref::<DownloadAbortError>().is_some() {
                        download_aborted = true;
                    }
                }
            }
            // 仅在发生风控时终止流，其它情况继续执行
            futures::future::ready(!download_aborted)
        })
        .filter_map(|res| futures::future::ready(res.ok()))
        .chunks(10);
    while let Some(models) = stream.next().await {
        update_pages_model(models, connection).await?;
    }
    if download_aborted {
        error!("下载视频「{}」的分页时触发风控，将异常向上传递..", &video_model.name);
        bail!(DownloadAbortError());
    }
    if error_occurred {
        error!(
            "下载视频「{}」的分页时出现错误，将在下一轮尝试重新处理",
            &video_model.name
        );
        bail!(ProcessPageError());
    }
    Ok(())
}

/// 下载某个分页，未发生风控且正常运行时返回 Ok(Page::ActiveModel)，其中 status 字段存储了新的下载状态，发生风控时返回 DownloadAbortError
///
/// 工作流程:
/// 1. 获取并发控制许可
/// 2. 获取分页状态
/// 3. 创建文件路径
/// 4. 并发下载分页资源:
///    - 分页封面
///    - 视频内容
///    - 分页NFO
///    - 弹幕文件
///    - 字幕文件
/// 5. 处理下载结果
/// 6. 更新分页状态
pub async fn download_page(
    bili_client: &BiliClient,  // B站API客户端
    video_model: &video::Model, // 视频模型
    page_model: page::Model,    // 分页模型
    semaphore: &Semaphore,     // 并发控制信号量
    downloader: &Downloader,    // 下载器
    base_path: &Path,          // 基础保存路径
) -> Result<page::ActiveModel> {
    // 获取并发控制许可
    let _permit = semaphore.acquire().await.context("acquire semaphore failed")?;
    
    // 获取分页下载状态
    let mut status = PageStatus::from(page_model.download_status);
    let separate_status = status.should_run();
    
    // 判断是否为单页视频
    let is_single_page = video_model.single_page.context("single_page is null")?;
    
    // 生成基础文件名
    let base_name = TEMPLATE.path_safe_render("page", &page_format_args(video_model, &page_model))?;
    
    // 根据是否为单页视频生成不同的文件路径
    let (poster_path, video_path, nfo_path, danmaku_path, fanart_path, subtitle_path) = if is_single_page {
        // 单页视频的文件路径格式
        (
            base_path.join(format!("{}-poster.jpg", &base_name)),
            base_path.join(format!("{}.mp4", &base_name)),
            base_path.join(format!("{}.nfo", &base_name)),
            base_path.join(format!("{}.zh-CN.default.ass", &base_name)),
            Some(base_path.join(format!("{}-fanart.jpg", &base_name))),
            base_path.join(format!("{}.srt", &base_name)),
        )
    } else {
        // 多页视频的文件路径格式(剧集格式)
        (
            base_path
                .join("Season 1")
                .join(format!("{} - S01E{:0>2}-thumb.jpg", &base_name, page_model.pid)),
            base_path
                .join("Season 1")
                .join(format!("{} - S01E{:0>2}.mp4", &base_name, page_model.pid)),
            base_path
                .join("Season 1")
                .join(format!("{} - S01E{:0>2}.nfo", &base_name, page_model.pid)),
            base_path
                .join("Season 1")
                .join(format!("{} - S01E{:0>2}.zh-CN.default.ass", &base_name, page_model.pid)),
            None, // 多页视频不需要单独的fanart
            base_path
                .join("Season 1")
                .join(format!("{} - S01E{:0>2}.srt", &base_name, page_model.pid)),
        )
    };

    // 构建视频分辨率信息
    let dimension = match (page_model.width, page_model.height) {
        (Some(width), Some(height)) => Some(Dimension {
            width,
            height,
            rotate: 0,
        }),
        _ => None,
    };

    // 构建页面信息
    let page_info = PageInfo {
        cid: page_model.cid,
        duration: page_model.duration,
        dimension,
        ..Default::default()
    };

    // 创建并发下载任务列表
    let tasks: Vec<Pin<Box<dyn Future<Output = Result<()>> + Send>>> = vec![
        Box::pin(fetch_page_poster(
            separate_status[0],
            video_model,
            &page_model,
            downloader,
            poster_path,
            fanart_path,
        )),
        Box::pin(fetch_page_video(
            // separate_status[1],
            false,
            bili_client,
            video_model,
            downloader,
            &page_info,
            &video_path,
        )),
        Box::pin(generate_page_nfo(
            separate_status[2],
            video_model,
            &page_model,
            nfo_path,
        )),
        Box::pin(fetch_page_danmaku(
            // separate_status[3],
            false,
            bili_client,
            video_model,
            &page_info,
            danmaku_path,
        )),
        Box::pin(fetch_page_subtitle(
            separate_status[4],
            bili_client,
            video_model,
            &page_info,
            &subtitle_path,
        )),
    ];

    // 按顺序执行下载任务
    let tasks: FuturesOrdered<_> = tasks.into_iter().collect();
    let results: Vec<Result<()>> = tasks.collect().await;
    
    // 更新下载状态
    status.update_status(&results);

    // 输出每个任务的执行结果
    results
        .iter()
        .zip(["封面", "视频", "详情", "弹幕", "字幕"])
        .for_each(|(res, task_name)| match res {
            Ok(_) => info!(
                "处理视频「{}」第 {} 页{}成功",
                &video_model.name, page_model.pid, task_name
            ),
            Err(e) => error!(
                "处理视频「{}」第 {} 页{}失败: {}",
                &video_model.name, page_model.pid, task_name, e
            ),
        });

    // 检查视频下载是否触发风控
    if let Err(e) = results.into_iter().nth(1).context("video download result not found")? {
        if let Ok(BiliError::RiskControlOccurred) = e.downcast::<BiliError>() {
            bail!(DownloadAbortError());
        }
    }

    // 更新分页模型状态
    let mut page_active_model: page::ActiveModel = page_model.into();
    page_active_model.download_status = Set(status.into());
    page_active_model.path = Set(Some(video_path.to_string_lossy().to_string()));
    Ok(page_active_model)
}

// 建议: 如何在下载分页资源时不下载"视频内容"和"弹幕文件"资源
// 1. 修改 PageStatus::should_run() 方法，让对应位置返回 false
// 2. 在创建 tasks 向量时，可以根据配置选择性地添加任务:
/*
let mut tasks: Vec<Pin<Box<dyn Future<Output = Result<()>> + Send>>> = Vec::new();

// 添加封面下载任务
tasks.push(Box::pin(fetch_page_poster(...)));

// 根据配置决定是否添加视频和弹幕下载任务
if config.download_video {
    tasks.push(Box::pin(fetch_page_video(...)));
}

// 添加NFO生成任务
tasks.push(Box::pin(generate_page_nfo(...)));

// 根据配置决定是否添加弹幕下载任务
if config.download_danmaku {
    tasks.push(Box::pin(fetch_page_danmaku(...)));
}

// 添加字幕下载任务
tasks.push(Box::pin(fetch_page_subtitle(...)));
*/

/// 下载分页封面图片
///
/// 对于单页视频使用视频封面,对于多页视频使用分页封面(如果有)或视频封面
pub async fn fetch_page_poster(
    should_run: bool,
    video_model: &video::Model,
    page_model: &page::Model,
    downloader: &Downloader,
    poster_path: PathBuf,
    fanart_path: Option<PathBuf>,
) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    let single_page = video_model.single_page.context("single_page is null")?;
    let url = if single_page {
        // 单页视频直接用视频的封面
        video_model.cover.as_str()
    } else {
        // 多页视频，如果单页没有封面，就使用视频的封面
        match &page_model.image {
            Some(url) => url.as_str(),
            None => video_model.cover.as_str(),
        }
    };
    downloader.fetch(url, &poster_path).await?;
    if let Some(fanart_path) = fanart_path {
        fs::copy(&poster_path, &fanart_path).await?;
    }
    Ok(())
}

/// 下载分页视频内容
///
/// 支持以下情况:
/// 1. 混合流(音视频在一起)
/// 2. 仅视频流
/// 3. 分离的音视频流(需要合并)
pub async fn fetch_page_video(
    should_run: bool,
    bili_client: &BiliClient,
    video_model: &video::Model,
    downloader: &Downloader,
    page_info: &PageInfo,
    page_path: &Path,
) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    let bili_video = Video::new(bili_client, video_model.bvid.clone());
    let streams = bili_video
        .get_page_analyzer(page_info)
        .await?
        .best_stream(&CONFIG.filter_option)?;
    match streams {
        BestStream::Mixed(mix_stream) => downloader.fetch(mix_stream.url(), page_path).await,
        BestStream::VideoAudio {
            video: video_stream,
            audio: None,
        } => downloader.fetch(video_stream.url(), page_path).await,
        BestStream::VideoAudio {
            video: video_stream,
            audio: Some(audio_stream),
        } => {
            let (tmp_video_path, tmp_audio_path) = (
                page_path.with_extension("tmp_video"),
                page_path.with_extension("tmp_audio"),
            );
            let res = async {
                downloader.fetch(video_stream.url(), &tmp_video_path).await?;
                downloader.fetch(audio_stream.url(), &tmp_audio_path).await?;
                downloader.merge(&tmp_video_path, &tmp_audio_path, page_path).await
            }
            .await;
            let _ = fs::remove_file(tmp_video_path).await;
            let _ = fs::remove_file(tmp_audio_path).await;
            res
        }
    }
}

/// 下载分页弹幕文件
pub async fn fetch_page_danmaku(
    should_run: bool,
    bili_client: &BiliClient,
    video_model: &video::Model,
    page_info: &PageInfo,
    danmaku_path: PathBuf,
) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    let bili_video = Video::new(bili_client, video_model.bvid.clone());
    bili_video
        .get_danmaku_writer(page_info)
        .await?
        .write(danmaku_path)
        .await
}

/// 下载分页字幕文件
///
/// 支持多语言字幕,每个语言保存为独立的文件
/// 
/// # 参数
/// - should_run: 是否执行下载,用于控制流程
/// - bili_client: B站API客户端
/// - video_model: 视频数据模型
/// - page_info: 分P信息
/// - subtitle_path: 字幕文件保存路径
pub async fn fetch_page_subtitle(
    should_run: bool,
    bili_client: &BiliClient,
    video_model: &video::Model,
    page_info: &PageInfo,
    subtitle_path: &Path,
) -> Result<()> {
    // 如果不需要执行,直接返回
    if !should_run {
        return Ok(());
    }
    
    // 创建视频对象并获取字幕列表
    let bili_video = Video::new(bili_client, video_model.bvid.clone());
    let subtitles = bili_video.get_subtitles(page_info).await?;
    
    // 并发下载所有语言的字幕
    let tasks = subtitles
        .into_iter()
        .map(|subtitle| async move {
            // 为每种语言生成独立的srt文件路径
            let path = subtitle_path.with_extension(format!("{}.srt", subtitle.lan));
            tokio::fs::write(path, subtitle.body.to_string()).await
        })
        .collect::<FuturesUnordered<_>>();
    
    // 等待所有字幕下载完成
    tasks.try_collect::<Vec<()>>().await?;
    Ok(())
}

/// 生成分P的NFO元数据文件
/// 
/// # 参数
/// - should_run: 是否执行生成
/// - video_model: 视频数据模型
/// - page_model: 分P数据模型
/// - nfo_path: NFO文件保存路径
pub async fn generate_page_nfo(
    should_run: bool,
    video_model: &video::Model,
    page_model: &page::Model,
    nfo_path: PathBuf,
) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    
    // 判断是否为单P视频,选择不同的NFO模式
    let single_page = video_model.single_page.context("single_page is null")?;
    let nfo_serializer = if single_page {
        // 单P视频使用MOVIE模式
        NFOSerializer(ModelWrapper::Video(video_model), NFOMode::MOVIE)
    } else {
        // 多P视频使用EPISODE模式
        NFOSerializer(ModelWrapper::Page(page_model), NFOMode::EPOSODE)
    };
    generate_nfo(nfo_serializer, nfo_path).await
}

/// 下载视频封面图片
/// 
/// 下载封面并复制一份作为fanart
pub async fn fetch_video_poster(
    should_run: bool,
    video_model: &video::Model,
    downloader: &Downloader,
    poster_path: PathBuf,
    fanart_path: PathBuf,
) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    // 下载封面图片
    downloader.fetch(&video_model.cover, &poster_path).await?;
    // 复制一份作为fanart
    fs::copy(&poster_path, &fanart_path).await?;
    Ok(())
}

/// 下载UP主头像
pub async fn fetch_upper_face(
    should_run: bool,
    video_model: &video::Model,
    downloader: &Downloader,
    upper_face_path: PathBuf,
) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    downloader.fetch(&video_model.upper_face, &upper_face_path).await
}

/// 生成UP主信息的NFO文件
pub async fn generate_upper_nfo(should_run: bool, video_model: &video::Model, nfo_path: PathBuf) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    let nfo_serializer = NFOSerializer(ModelWrapper::Video(video_model), NFOMode::UPPER);
    generate_nfo(nfo_serializer, nfo_path).await
}

/// 生成视频信息的NFO文件
pub async fn generate_video_nfo(should_run: bool, video_model: &video::Model, nfo_path: PathBuf) -> Result<()> {
    if !should_run {
        return Ok(());
    }
    let nfo_serializer = NFOSerializer(ModelWrapper::Video(video_model), NFOMode::TVSHOW);
    generate_nfo(nfo_serializer, nfo_path).await
}

/// 创建 nfo_path 的父目录，然后写入 nfo 文件
/// 
/// # 参数
/// - serializer: NFO序列化器
/// - nfo_path: NFO文件保存路径
async fn generate_nfo(serializer: NFOSerializer<'_>, nfo_path: PathBuf) -> Result<()> {
    // 确保父目录存在
    if let Some(parent) = nfo_path.parent() {
        fs::create_dir_all(parent).await?;
    }
    // 生成并写入NFO内容
    fs::write(
        nfo_path,
        serializer.generate_nfo(&CONFIG.nfo_time_type).await?.as_bytes(),
    )
    .await?;
    Ok(())
}

/// 单元测试模块
#[cfg(test)]
mod tests {
    use handlebars::handlebars_helper;
    use serde_json::json;

    use super::*;

    /// 测试模板使用功能
    #[test]
    fn test_template_usage() {
        // 初始化Handlebars模板引擎
        let mut template = handlebars::Handlebars::new();
        
        // 注册truncate辅助函数用于截断字符串
        handlebars_helper!(truncate: |s: String, len: usize| {
            if s.chars().count() > len {
                s.chars().take(len).collect::<String>()
            } else {
                s.to_string()
            }
        });
        template.register_helper("truncate", Box::new(truncate));
        
        // 注册各种测试模板
        let _ = template.path_safe_register("video", "test{{bvid}}test");
        let _ = template.path_safe_register("test_truncate", "哈哈，{{ truncate title 30 }}");
        let _ = template.path_safe_register("test_path_unix", "{{ truncate title 7 }}/test/a");
        let _ = template.path_safe_register("test_path_windows", r"{{ truncate title 7 }}\\test\\a");
        
        // 针对不同操作系统测试路径处理
        #[cfg(not(windows))]
        {
            assert_eq!(
                template
                    .path_safe_render("test_path_unix", &json!({"title": "关注/永雏塔菲喵"}))
                    .unwrap(),
                "关注_永雏塔菲/test/a"
            );
            assert_eq!(
                template
                    .path_safe_render("test_path_windows", &json!({"title": "关注/永雏塔菲喵"}))
                    .unwrap(),
                "关注_永雏塔菲_test_a"
            );
        }
        #[cfg(windows)]
        {
            assert_eq!(
                template
                    .path_safe_render("test_path_unix", &json!({"title": "关注/永雏塔菲喵"}))
                    .unwrap(),
                "关注_永雏塔菲_test_a"
            );
            assert_eq!(
                template
                    .path_safe_render("test_path_windows", &json!({"title": "关注/永雏塔菲喵"}))
                    .unwrap(),
                r"关注_永雏塔菲\\test\\a"
            );
        }
        
        // 测试基本模板渲染
        assert_eq!(
            template
                .path_safe_render("video", &json!({"bvid": "BV1b5411h7g7"}))
                .unwrap(),
            "testBV1b5411h7g7test"
        );
        
        // 测试长文本截断
        assert_eq!(
            template
                .path_safe_render(
                    "test_truncate",
                    &json!({"title": "你说得对，但是 Rust 是由 Mozilla 自主研发的一款全新的编译期格斗游戏。\
                    编译将发生在一个被称作「Cargo」的构建系统中。在这里，被引用的指针将被授予「生命周期」之力，导引对象安全。\
                    你将扮演一位名为「Rustacean」的神秘角色, 在与「Rustc」的搏斗中邂逅各种骨骼惊奇的傲娇报错。\
                    征服她们、通过编译同时，逐步发掘「C++」程序崩溃的真相。"})
                )
                .unwrap(),
            "哈哈，你说得对，但是 Rust 是由 Mozilla 自主研发的一"
        );
    }
}
