use aws_sdk_s3::operation::{
    get_object::{GetObjectError, GetObjectOutput},
    put_object::{PutObjectError, PutObjectOutput},
};
use aws_sdk_s3::{error::SdkError, primitives::ByteStream, Client};
use dicom_object::{open_file, Tag};
use dicom_pixeldata::PixelDecoder;
use image::{save_buffer_with_format, ColorType::L8, ImageFormat::Jpeg};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use tracing::{error, info};

pub fn collect_tag_data(file_path: String, tag: Tag) -> String {
    let obj = open_file(file_path.clone()).unwrap_or_else(|e| {
        error!("Error reading `{}`, {}", file_path, e);
        std::process::exit(1);
    });
    obj.element(tag)
        .unwrap_or_else(|e| {
            error!("Access error on tag`{}` for `{}`, {}", tag, file_path, e);
            std::process::exit(1);
        })
        .to_str()
        .unwrap_or_else(|e| {
            error!("Convert on tag`{}` for `{}`, {}", tag, file_path, e);
            std::process::exit(1);
        })
        .to_string()
}

pub fn save_image_data(file_in: String, file_out: String) -> Result<(), image::ImageError> {
    let obj = open_file(file_in.clone()).unwrap_or_else(|e| {
        error!("Error reading `{}`, {}", file_in, e);
        std::process::exit(1);
    });
    let image = obj.decode_pixel_data().unwrap_or_else(|e| {
        error!("Problem decoding image `{}`, {}", file_in, e);
        std::process::exit(1);
    });
    let width = image.columns();
    let height = image.rows();
    let number_of_frames = image.number_of_frames();
    // Current setting is hardcoded to Luma8 grayscale and JPEG with 100% quality for compression
    let bytes = (0..(number_of_frames))
        .into_par_iter()
        .flat_map(|frame| image.to_dynamic_image(frame).unwrap().to_luma8().to_vec())
        .collect::<Vec<u8>>();

    save_buffer_with_format(&file_out, &bytes, width, height, L8, Jpeg)
}

pub async fn instantiate_dir(path: &Path) -> std::io::Result<PathBuf> {
    let mut path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        if !path.is_file() && !path.is_dir() {
            // If the specified destination directory doesn't already exist,
            // see if the directory structure needs to be made
            let dir = if !path.to_string_lossy().ends_with('/') {
                path.pop();
                path
            } else {
                path
            };
            if !dir.exists() && !dir.as_os_str().is_empty() {
                info!("Creating directory path {:?}", dir.as_os_str());
                std::fs::create_dir_all(&dir)?;
            }
            Ok(dir)
        } else {
            path.pop();
            Ok(path)
        }
    })
    .await?
}

pub async fn download_object(
    client: &Client,
    bucket_name: &str,
    key: &str,
) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
    client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
}

pub async fn upload_object(
    client: &Client,
    bucket_name: &str,
    file_name: &str,
    key: &str,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    let body = ByteStream::from_path(Path::new(file_name)).await;
    client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(body.unwrap())
        .send()
        .await
}
