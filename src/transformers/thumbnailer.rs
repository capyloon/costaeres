use crate::array::Array;
/// Thumbnailer transformer.
use crate::common::{Variant, VariantMetadata};
use crate::transformers::{
    TransformFnResult, TransformationResult, VariantChange, VariantTransformer,
};
use async_std::io::{ReadExt, SeekFrom};
use futures::{future, AsyncSeekExt};
use image::io::Reader as ImageReader;
use log::{error, info};
use std::io::Cursor;
use std::ops::DerefMut;

const DEFAULT_THUMBNAIL_SIZE: u32 = 128;

pub struct Thumbnailer {
    size: u32,
}

impl Default for Thumbnailer {
    fn default() -> Self {
        Self {
            size: DEFAULT_THUMBNAIL_SIZE,
        }
    }
}

fn err_nop<T: std::error::Error>(e: T) -> () {
    error!("Unexpected: {:?}", e);
    ()
}

async fn create_thumbnail(variant: &mut Variant, size: u32) -> Result<Variant, ()> {
    let content = &mut variant.reader;
    content.seek(SeekFrom::Start(0)).await.map_err(err_nop)?;
    let mut buffer = vec![];
    content.read_to_end(&mut buffer).await.map_err(err_nop)?;
    content.seek(SeekFrom::Start(0)).await.map_err(err_nop)?;

    info!("image size is {}", buffer.len());
    let img = ImageReader::new(Cursor::new(buffer))
        .with_guessed_format()
        .map_err(err_nop)?
        .decode()
        .map_err(err_nop)?;

    info!(
        "Creating thumbnail for image {}x{}",
        img.width(),
        img.height()
    );

    let thumbnail = img.thumbnail(size, size);

    let mut bytes: Vec<u8> = Vec::new();
    thumbnail
        .write_to(
            &mut Cursor::new(&mut bytes),
            image::ImageOutputFormat::Jpeg(90),
        )
        .map_err(err_nop)?;

    let v = Variant::new(
        VariantMetadata::new("thumbnail", "image/jpeg", bytes.len() as _),
        Box::new(Array::new(bytes)),
    );

    Ok(v)
}

impl VariantTransformer for Thumbnailer {
    fn transform_variant(&self, change: &mut VariantChange) -> TransformFnResult {
        let meta = &change.metadata;

        // Only process default variants of image/*  mime type.
        let res = if meta.name() == "default" && meta.mime_type().starts_with("image/") {
            if change.is_deleted() {
                TransformationResult::Delete("thumbnail".into())
            } else {
                info!(
                    "Will create thumbnail for variant with mimeType '{}'",
                    meta.mime_type()
                );
                let size = self.size;
                async_std::task::block_on(async {
                    // Return a new variant.
                    if let Ok(v) = create_thumbnail(change.deref_mut(), size).await {
                        match change {
                            VariantChange::Created(_) => {
                                info!("Thumbnail variant created");
                                TransformationResult::Create(v)
                            }
                            VariantChange::Updated(_) => {
                                info!("Thumbnail variant updated");
                                TransformationResult::Update(v)
                            }
                            _ => panic!("Unexpected variant change!"),
                        }
                    } else {
                        TransformationResult::Noop
                    }
                })
            }
        } else {
            TransformationResult::Noop
        };

        Box::pin(future::ready(vec![res]))
    }
}
