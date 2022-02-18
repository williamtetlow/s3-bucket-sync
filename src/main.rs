use anyhow::{Context, Error};
use aws_sdk_s3::{model::Object, ByteStream, Client, SdkError};
use base64ct::{Base64, Encoding};
use bytes::Buf;
use futures::{stream, StreamExt};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env, fs, io,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time;
use walkdir::{DirEntry, WalkDir};

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    aws_access_key_id: String,
    aws_secret_access_key: String,
    aws_region: String,
    bucket: String,
    max_concurrent_requests: u8,
    sync_interval_in_seconds: u64,
    root: PathBuf,
}

#[derive(Debug)]
struct SyncOptions {
    bucket: String,
    max_concurrent_requests: u8,
    root: PathBuf,
}

trait S3Key {
    fn get_s3_key(&self, root: &Path) -> Result<String, Error>;

    fn from_s3_key(key: &str, root: &Path) -> PathBuf;
}

impl S3Key for PathBuf {
    fn get_s3_key(&self, root: &Path) -> Result<String, Error> {
        self.strip_prefix(root)
            .map(|path| path.to_string_lossy().into_owned())
            .map_err(anyhow::Error::from)
    }

    fn from_s3_key(key: &str, root: &Path) -> Self {
        root.join(key)
    }
}

#[derive(Default)]
struct Diff {
    both: Vec<PathBuf>,
    dest_only: Vec<PathBuf>,
    src_only: Vec<PathBuf>,
}

fn collect_src(root: &Path) -> HashSet<PathBuf> {
    let walker = WalkDir::new(root).into_iter();

    let iter = walker
        .filter_entry(|e| !is_hidden(e))
        .filter_map(|e| match e {
            Ok(dir_entry) => {
                if is_file(&dir_entry) {
                    Some(dir_entry.into_path())
                } else {
                    None
                }
            }
            Err(err) => {
                log::warn!("failed to read dir entry {}", err);
                None
            }
        });

    HashSet::from_iter(iter)
}

async fn collect_remote(client: &Client, bucket: &str) -> Vec<Object> {
    let mut dest = vec![];

    let mut continuation_token = None;

    loop {
        let page = client
            .list_objects_v2()
            .set_continuation_token(continuation_token)
            .bucket(bucket)
            .send()
            .await
            .unwrap();

        if let Some(contents) = page.contents() {
            dest.extend_from_slice(contents);
        }

        if page.is_truncated() {
            continuation_token = page.continuation_token().map(String::from);
        } else {
            break;
        }
    }

    dest
}

async fn generate_diff(client: &Client, opts: &SyncOptions) -> Diff {
    let mut src = collect_src(&opts.root);
    let remote = collect_remote(client, &opts.bucket).await;

    let (both, dest_only): (Vec<PathBuf>, Vec<PathBuf>) = remote
        .into_iter()
        .filter_map(|obj| obj.key().map(|key| PathBuf::from_s3_key(key, &opts.root)))
        .partition(|remote_path| src.remove(remote_path));

    Diff {
        both,
        dest_only,
        src_only: Vec::from_iter(src),
    }
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

fn is_file(entry: &DirEntry) -> bool {
    entry
        .metadata()
        .map(|metadata| metadata.is_file())
        .unwrap_or(false)
}

fn get_md5_of_file(file_path: &Path) -> Result<String, Error> {
    let file = std::fs::File::open(file_path)?;

    let mut bufreader = io::BufReader::new(file);

    let mut hasher = Md5::new();

    io::copy(&mut bufreader, &mut hasher)
        .context("failed to hash file")
        .map_err(anyhow::Error::from)?;

    Ok(Base64::encode_string(&hasher.finalize()))
}

const MD5_METADATA_KEY: &str = "x-amz-meta-md5";

async fn check_s3_object_matches_local(
    client: &Client,
    bucket: &str,
    key: &str,
    md5: &str,
) -> Result<bool, Error> {
    let head_result = client.head_object().bucket(bucket).key(key).send().await;

    let s3_object_matches_local = head_result
        .map(Some)
        .or_else(|e| match &e {
            SdkError::ServiceError { err, .. } => {
                if err.is_not_found() {
                    // if we get a NotFoundError we don't want to fail
                    // it just means we've not uploaded the object before
                    Ok(None)
                } else {
                    Err(e)
                }
            }
            _ => Err(e),
        })?
        .as_ref()
        .and_then(|head| head.metadata())
        .and_then(|metadata| metadata.get(MD5_METADATA_KEY))
        .map(|remote_md5| md5 == *remote_md5)
        .unwrap_or(false);

    Ok(s3_object_matches_local)
}

async fn upload_object_to_s3(
    client: &Client,
    file: &Path,
    bucket: &str,
    key: &str,
    md5: String,
) -> Result<(), Error> {
    let body = ByteStream::from_path(&file).await?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .set_content_md5(Some(md5.to_owned()))
        .set_metadata(Some(HashMap::from([(MD5_METADATA_KEY.to_owned(), md5)])))
        .body(body)
        .send()
        .await?;

    Ok(())
}

async fn download_object_from_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    local_path: &Path,
) -> Result<(), Error> {
    let resp = client.get_object().bucket(bucket).key(key).send().await?;

    let mut data = resp.body.collect().await?.reader();

    let mut file = fs::File::create(local_path)?;

    io::copy(&mut data, &mut file)?;

    Ok(())
}

async fn upload_object_to_s3_if_changed(
    client: &Client,
    file: &Path,
    bucket: &str,
    key: &str,
) -> Result<(), Error> {
    let md5 = get_md5_of_file(&file)?;

    let s3_object_matches_local = check_s3_object_matches_local(client, bucket, key, &md5).await?;

    if s3_object_matches_local {
        log::debug!(
            "{:?} local version matches S3 object - skipping upload",
            &file
        );
        return Ok(());
    }

    upload_object_to_s3(client, file, bucket, key, md5).await
}

async fn get_dest_only_objects(client: &Client, paths: Vec<PathBuf>, opts: &SyncOptions) {
    if paths.is_empty() {
        log::info!("There are no dest_only objects to download - skipping");
        return;
    }

    log::info!("Downloading {} dest_only objects", paths.len());

    stream::iter(paths)
        .map(|path| async move {
            download_object_from_s3(client, &opts.bucket, &path.get_s3_key(&opts.root)?, &path)
                .await
        })
        .buffer_unordered(opts.max_concurrent_requests.into())
        .for_each(|resp| async {
            if let Err(err) = resp {
                log::error!("Failed to download dest_only object {}", err);
            } else {
                log::debug!("Downloaded dest_only object");
            }
        })
        .await;
}

async fn put_src_only_objects(client: &Client, paths: Vec<PathBuf>, opts: &SyncOptions) {
    if paths.is_empty() {
        log::info!("There are no src_only objects to upload - skipping");
        return;
    }

    log::info!("Uploading {} src_only objects", paths.len());

    stream::iter(paths)
        .map(|path| async move {
            let md5 = get_md5_of_file(&path)?;

            upload_object_to_s3(
                client,
                &path,
                &opts.bucket,
                &path.get_s3_key(&opts.root)?,
                md5,
            )
            .await
        })
        .buffer_unordered(opts.max_concurrent_requests.into())
        .for_each(|resp| async {
            if let Err(err) = resp {
                log::error!("Failed to upload src_only object {}", err);
            } else {
                log::debug!("Uploaded src_only object");
            }
        })
        .await;
}

async fn sync_objects_on_src_and_dest(client: &Client, paths: Vec<PathBuf>, opts: &SyncOptions) {
    if paths.is_empty() {
        log::info!("There are no objects to sync - skipping");
        return;
    }

    log::info!("Syncing {} objects", paths.len());

    stream::iter(paths)
        .map(|path| async move {
            // TODO: handle deleted objects
            upload_object_to_s3_if_changed(
                client,
                &path,
                &opts.bucket,
                &path.get_s3_key(&opts.root)?,
            )
            .await
        })
        .buffer_unordered(opts.max_concurrent_requests.into())
        .for_each(|resp| async {
            if let Err(err) = resp {
                log::error!("Failed to sync object {}", err);
            } else {
                log::debug!("Object sync complete");
            }
        })
        .await;
}

async fn sync_folder_to_s3(client: &Client, opts: &SyncOptions) -> Result<(), Error> {
    let diff = generate_diff(client, &opts).await;

    get_dest_only_objects(client, diff.dest_only, &opts).await;
    put_src_only_objects(client, diff.src_only, &opts).await;
    sync_objects_on_src_and_dest(client, diff.both, &opts).await;

    Ok(())
}

fn get_config() -> Result<Config, Error> {
    let file = std::fs::read_to_string(".sync-config.toml")?;

    let config: Config = toml::from_str(&file)?;

    Ok(config)
}

async fn get_aws_config(config: &Config) -> aws_config::Config {
    env::set_var("AWS_ACCESS_KEY_ID", &config.aws_access_key_id);
    env::set_var("AWS_SECRET_ACCESS_KEY", &config.aws_secret_access_key);
    env::set_var("AWS_REGION", &config.aws_region);

    aws_config::load_from_env().await
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let config = get_config().context("failed to read config").unwrap();
    let aws_config = get_aws_config(&config).await;
    let client = Client::new(&aws_config);

    let Config {
        bucket,
        max_concurrent_requests,
        sync_interval_in_seconds,
        root,
        ..
    } = config;

    let sync_opts = SyncOptions {
        bucket,
        max_concurrent_requests,
        root,
    };

    loop {
        sync_folder_to_s3(&client, &sync_opts)
            .await
            .context("failed to sync folder to S3")
            .unwrap();

        time::sleep(Duration::from_secs(sync_interval_in_seconds)).await;
    }
}
