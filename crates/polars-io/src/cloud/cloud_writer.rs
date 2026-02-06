use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use object_store::PutPayload;
use polars_utils::async_utils::error_capture::{ErrorCapture, ErrorHandle};
use polars_utils::async_utils::tokio_handle_ext;

use crate::cloud::PolarsObjectStore;
use crate::metrics::{IOMetrics, OptIOMetrics};
use crate::{get_upload_chunk_size, get_upload_concurrency};

pub struct CloudWriter {
    store: PolarsObjectStore,
    path: object_store::path::Path,
    max_concurrency: NonZeroUsize,
    io_metrics: OptIOMetrics,

    multipart: Option<Box<dyn object_store::MultipartUpload>>,
    tasks: FuturesUnordered<tokio_handle_ext::AbortOnDropHandle<()>>,
    error_handle: Option<ErrorHandle<object_store::Error>>,
    error_capture: ErrorCapture<object_store::Error>,
}

impl CloudWriter {
    pub fn new(
        store: PolarsObjectStore,
        path: object_store::path::Path,
        max_concurrency: NonZeroUsize,
        io_metrics: Option<Arc<IOMetrics>>,
    ) -> Self {
        let (error_capture, error_handle) = ErrorCapture::new();

        Self {
            store,
            path,
            max_concurrency,
            io_metrics: OptIOMetrics(io_metrics),

            multipart: None,
            tasks: FuturesUnordered::new(),
            error_handle: Some(error_handle),
            error_capture,
        }
    }

    pub fn as_buffered<'a>(
        &'a mut self,
        buffer_chunk_size: NonZeroUsize,
    ) -> BufferedCloudWriter<'a> {
        BufferedCloudWriter {
            writer: self,
            buffer_chunk_size,
            buffered: vec![],
            num_bytes_buffered: 0,
        }
    }

    async fn get_or_init_multipart(
        &mut self,
    ) -> object_store::Result<&mut (dyn object_store::MultipartUpload + 'static)> {
        if self.multipart.is_none() {
            let multipart = self
                .store
                .to_dyn_object_store()
                .await
                .put_multipart_opts(&self.path, object_store::PutMultipartOptions::default())
                .await?;

            self.multipart = Some(multipart)
        }

        Ok(self.multipart.as_deref_mut().unwrap())
    }

    pub async fn put(&mut self, payload: PutPayload) -> object_store::Result<()> {
        if self.error_handle.as_ref().unwrap().has_errored() {
            return Err(self.error_handle.take().unwrap().join().await.unwrap_err());
        }

        if self.tasks.len() >= self.max_concurrency.get() {
            self.tasks.next().await;
        }

        debug_assert!(self.tasks.len() < self.max_concurrency.get());

        let io_metrics = self.io_metrics.clone();
        let num_bytes = payload.content_length() as u64;
        let upload_fut = self.get_or_init_multipart().await?.put_part(payload);

        let fut = async move { io_metrics.record_bytes_tx(num_bytes, upload_fut).await };

        let handle = tokio_handle_ext::AbortOnDropHandle(tokio::spawn(
            self.error_capture.clone().wrap_future(fut),
        ));

        self.tasks.push(handle);

        Ok(())
    }

    pub async fn finish(&mut self) -> object_store::Result<()> {
        self.multipart.take().unwrap().complete().await?;
        self.error_handle.take().unwrap().join().await?;

        Ok(())
    }
}

pub struct BufferedCloudWriter<'a> {
    writer: &'a mut CloudWriter,
    buffer_chunk_size: NonZeroUsize,
    buffered: Vec<bytes::Bytes>,
    num_bytes_buffered: u64,
}

impl BufferedCloudWriter<'_> {
    pub async fn put(
        &mut self,
        bytes: impl IntoIterator<Item = bytes::Bytes>,
    ) -> object_store::Result<()> {
    }
}
