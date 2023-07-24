
use pin_project::pin_project; // 0.4.17
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{self, Context, Poll},
    thread,
    time::Duration,
};
use tokio::time;


#[pin_project]
#[derive(Debug)]
struct Cancelable<F> {
    #[pin]
    inner: F,
    info: Arc<Mutex<CancelInfo>>,
}

#[derive(Debug, Default)]
struct CancelInfo {
    cancelled: bool,
    task: Option<task::Waker>,
}

impl<F> Cancelable<F> {
    fn new(inner: F) -> (Self, impl FnOnce()) {
        let info = Arc::new(Mutex::new(CancelInfo::default()));
        let cancel = {
            let info = info.clone();
            move || {
                let mut info = info.lock().unwrap();
                info.cancelled = true;
                if let Some(waker) = info.task.take() {
                    waker.wake();
                }
            }
        };
        let me = Cancelable { inner, info };
        (me, cancel)
    }
}

impl<F> Future for Cancelable<F>
where
    F: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut info = this.info.lock().unwrap();

        if info.cancelled {
            Poll::Ready(())
        } else {
            let r = this.inner.poll(ctx);

            if r.is_pending() {
                info.task = Some(ctx.waker().clone());
            }

            r
        }
    }
}