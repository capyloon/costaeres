/// Variant transformers provide optional processing of variants
/// during their lifecycle.
/// Example use cases:
/// - generate thumbnails from full size images.
/// - generate vcard from contacts.
use crate::common::Variant;
use core::pin::Pin;

pub mod thumbnailer;

pub enum VariantChange<'a> {
    Created(&'a mut Variant),
    Updated(&'a mut Variant),
    Deleted(&'a mut Variant),
}

impl<'a> VariantChange<'a> {
    pub fn is_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }

    pub fn is_updated(&self) -> bool {
        matches!(self, Self::Updated(_))
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }
}

impl<'a> std::ops::Deref for VariantChange<'a> {
    type Target = Variant;

    fn deref(&self) -> &Variant {
        match self {
            Self::Created(v) | Self::Updated(v) | Self::Deleted(v) => v,
        }
    }
}

impl<'a> std::ops::DerefMut for VariantChange<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Created(v) | Self::Updated(v) | Self::Deleted(v) => v,
        }
    }
}

#[derive(Debug)]
pub enum TransformationResult {
    Noop,
    Delete(String), // the variant name.
    Create(Variant),
    Update(Variant),
}

pub type TransformFnResult = Pin<Box<dyn futures_core::Future<Output = Vec<TransformationResult>>>>;

pub trait VariantTransformer {
    fn transform_variant(&self, change: &mut VariantChange) -> TransformFnResult;
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::common::{Variant, VariantMetadata};
    use async_std::fs;
    use futures::future;

    use super::{TransformationResult, VariantTransformer};

    fn named_variant(name: &str, mime_type: &str) -> VariantMetadata {
        VariantMetadata::new(name, mime_type, 42)
    }

    async fn named_content(name: &str, mime: &str) -> Variant {
        let file = fs::File::open("./create_db.sh").await.unwrap();
        Variant::new(named_variant(name, mime), Box::new(file))
    }

    fn expect_noop(r: &TransformationResult) {
        if let TransformationResult::Noop = r {
            assert!(true);
        } else {
            assert!(false);
        }
    }

    fn expect_delete(r: &TransformationResult, name: &str) {
        if let TransformationResult::Delete(v_name) = r {
            assert_eq!(name, v_name);
        } else {
            assert!(false);
        }
    }

    fn expect_create(r: &TransformationResult, name: &str) {
        if let TransformationResult::Create(variant) = r {
            assert_eq!(name, &variant.metadata.name());
        } else {
            assert!(false);
        }
    }

    struct NoopTransformer {}

    // #[async_trait(?Send)]
    impl VariantTransformer for NoopTransformer {
        fn transform_variant(&self, _change: &mut VariantChange) -> TransformFnResult {
            Box::pin(future::ready(vec![TransformationResult::Noop]))
        }
    }

    #[async_std::test]
    async fn noop_transform() {
        let mut v = named_content("default", "text/plain").await;
        let t = NoopTransformer {};
        let r = t
            .transform_variant(&mut VariantChange::Created(&mut v))
            .await;
        expect_noop(&r[0]);
    }

    struct Thumbnailer {}
    impl VariantTransformer for Thumbnailer {
        fn transform_variant(&self, change: &mut VariantChange) -> TransformFnResult {
            let meta = &change.metadata;

            // Only process default variants of image/*  mime type.
            let res = if meta.name() == "default" && meta.mime_type().starts_with("image/") {
                if change.is_deleted() {
                    TransformationResult::Delete("thumbnail".into())
                } else {
                    async_std::task::block_on(async {
                        // Return a new variant.
                        let v = named_content("thumbnail", "image/png").await;
                        match change {
                            VariantChange::Created(_) => TransformationResult::Create(v),
                            VariantChange::Updated(_) => TransformationResult::Update(v),
                            _ => panic!("Unexpected variant change!"),
                        }
                    })
                }
            } else {
                TransformationResult::Noop
            };

            Box::pin(future::ready(vec![res]))
        }
    }

    #[async_std::test]
    async fn thumbnail_transform() {
        let t = Thumbnailer {};

        // Failure: not an image type.
        let mut v = named_content("default", "text/plain").await;
        let r = t
            .transform_variant(&mut VariantChange::Created(&mut v))
            .await;
        assert_eq!(r.len(), 1);
        expect_noop(&r[0]);

        // Failure: not the default variant.
        let mut v = named_content("icon", "image/png").await;
        let r = t
            .transform_variant(&mut VariantChange::Created(&mut v))
            .await;
        assert_eq!(r.len(), 1);
        expect_noop(&r[0]);

        // Deleting the default image -> deleting the thumbnail.
        let mut v = named_content("default", "image/png").await;
        let r = t
            .transform_variant(&mut VariantChange::Deleted(&mut v))
            .await;
        assert_eq!(r.len(), 1);
        expect_delete(&r[0], "thumbnail");

        // Create a thumbnail
        let mut v = named_content("default", "image/png").await;
        let r = t
            .transform_variant(&mut VariantChange::Created(&mut v))
            .await;
        assert_eq!(r.len(), 1);
        expect_create(&r[0], "thumbnail");
    }
}
