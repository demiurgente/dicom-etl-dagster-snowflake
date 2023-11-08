use chrono::NaiveDate;
use dicom_dictionary_std::tags;
use img_compressor::*;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use lazy_static::lazy_static;
use rayon::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;
use tracing::{error, info};
lazy_static! {
    static ref DIR_IN: String = "test-data/downloads/".to_string();
    static ref DIR_OUT: String = "test-data/uploads/".to_string();
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ImgCompressorConfig {
    #[serde(rename = "source_bucket")]
    pub source_bucket: String, // source S3 bucket to pull files from
    #[serde(rename = "source_prefix")]
    pub source_prefix: String, // Prefix leading to folder with DICOM files
    #[serde(rename = "target_bucket")]
    pub target_bucket: String, // target S3 bucket to push files to
    #[serde(rename = "target_prefix")]
    pub target_prefix: String, // future folder for compressed image
    #[serde(rename = "files")]
    pub files: Vec<String>, // List of dicom files as keys on s3 with prefix
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    lambda_runtime::run(service_fn(handler)).await
}

async fn handler(event: LambdaEvent<ImgCompressorConfig>) -> Result<(), Error> {
    // create temporary file holders for download / upload
    let _ = instantiate_dir(Path::new(&DIR_IN.clone())).await;
    let _ = instantiate_dir(Path::new(&DIR_OUT.clone())).await;

    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);
    let s3_cli = Arc::new(Mutex::new(client));

    // list of dicom files as keys on s3 with prefix
    let files = event.payload.files;
    let src_bucket = event.payload.source_bucket;
    let src_prefix = event.payload.source_prefix;
    let dst_bucket = event.payload.target_bucket;
    let dst_prefix = event.payload.target_prefix;

    // download s3 files, compress images to JPEG and upload back to s3 in parallel
    files.par_iter().for_each(|file| {
        let file = file.clone();
        let s3_cli = s3_cli.clone();
        let src_bucket = src_bucket.clone();
        let src_prefix = src_prefix.clone();
        let dst_bucket = dst_bucket.clone();
        let dst_prefix = dst_prefix.clone();

        tokio::spawn(async move {
            let client = s3_cli.lock().await;
            let bucket = src_bucket.clone();
            let prefix = src_prefix.clone();
            let s3_file = format!("{}/{}", prefix, file);

            match download_object(&client, &bucket, &s3_file).await {
                Ok(_) => info!("Processing s3 file: `{s3_file}`"),
                Err(e) => {
                    error!("Error pulling s3 file: `{s3_file}`, {e}");
                    std::process::exit(1);
                }
            }

            let file_i = format!("{}{}", DIR_IN.clone(), file);
            let sop_instance_uid = collect_tag_data(file_i.clone(), tags::SOP_INSTANCE_UID);
            let series_instance_uid = collect_tag_data(file_i.clone(), tags::SERIES_INSTANCE_UID);
            let instance_creation_date =
                collect_tag_data(file_i.clone(), tags::INSTANCE_CREATION_DATE);

            let file_name = format!("{series_instance_uid}|{sop_instance_uid}.jpeg");
            let file_o = format!("{}{}", DIR_OUT.clone(), file_name);

            save_image_data(file_i.clone(), file_o.clone()).unwrap_or_else(|e| {
                error!("Error saving image for `{file_i}`, {e}");
                std::process::exit(1);
            });

            // improvized partition write to different folders on s3 grained to a day
            let partition_date = NaiveDate::parse_from_str(&instance_creation_date, "%Y%m%d")
                .unwrap_or_else(|e| {
                    error!("Parse error for date for `{file}`, {e}");
                    std::process::exit(1);
                })
                .format("%Y_%m");

            // upload file back to prefix of the form <dst_bucket>/<dst_prefix>/%Y_%m/%d/<s3_key>.jpeg
            let s3_key = format!("{dst_prefix}/compressed_images/{partition_date}/{file_name}",);

            match upload_object(&client, &dst_bucket, &file_name, &s3_file).await {
                Ok(_) => info!("Successful upload s3 file: `s3://{dst_bucket}/{s3_key}`"),
                Err(e) => {
                    error!("Error upload from `{s3_file}`, to `s3://{dst_bucket}/{s3_key}`, {e}");
                    std::process::exit(1);
                }
            }
        });
    });
    Ok(())
}
