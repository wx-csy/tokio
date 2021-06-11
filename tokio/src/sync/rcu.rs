use std::ops::Deref;
use std::task::Poll;
use std::future::Future;

use crate::loom::sync::atomic::AtomicPtr;
use crate::loom::sync::atomic::Ordering;

/// An RCU pointer.
#[derive(Debug)]
pub struct Rcu<T: Send> {
    data: AtomicPtr<T>
}

unsafe impl <T: Send> Send for Rcu<T> {}
unsafe impl <T: Send + Sync> Sync for Rcu<T> {}

impl<T: Send> Rcu<T> {
    /// Run sychronization task on all workers, ensuring grace periods are expired.
    async fn synchronize_rcu() {
        #[derive(Clone)]
        struct RcuSynchronizeTask;

        impl Future for RcuSynchronizeTask {
            type Output = ();

            fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                Poll::Ready(())
            }
        }

        let tasks = crate::task::spawn_all(RcuSynchronizeTask);
        for task in Vec::from(tasks) {
            task.await.expect("error when running rcu synchronization tasks");
        }
    }
    
    /// Create a new RCU pointer.
    pub unsafe fn new(data: T) -> Rcu<T> {
        Rcu {
            data: AtomicPtr::new(Box::into_raw(data.into()))
        }
    }

    /// Read a the current RCU pointer. The returned reference is immutable and `!Send`.
    pub fn read(&self) -> RcuReference<T> {
        RcuReference {
            data: self.data.load(Ordering::Acquire) as *const T,
        }
    }

    /// Update the current RCU pointer. The value is updated immediately, and a synchronize handle is
    /// returned for retrieving the old value. The handle must not be dropped
    pub fn update(&self, new: T) -> RcuSyncHandle<T>  {
        self.update_boxed(Box::new(new))
    }

    /// Update the current RCU pointer with boxed value. Wait for all readers leaving the grace period,
    /// then return the old boxed value.
    pub fn update_boxed(&self, new: Box<T>) -> RcuSyncHandle<T> {
        RcuSyncHandle {
            data: self.data.swap(Box::into_raw(new), Ordering::AcqRel)
        }
    }

    /// Consume this `Rcu`. Return the inner boxed value.
    pub fn into_boxed(self) -> Box<T> {
        unsafe { Box::from_raw(self.data.load(Ordering::Acquire)) }
    }

    /// Consume this `Rcu`. Return the inner value.
    pub fn into_inner(self) -> T {
        // Boxed value can be unboxed by `*`. This is specially supported by Rust compiler.
        *self.into_boxed()
    }

    /// Consume this `Rcu`. Return the raw pointer for the inner value.
    pub fn into_raw(self) -> *mut T {
        // Boxed value can be unboxed by `*`. This is specially supported by Rust compiler.
        self.data.load(Ordering::Acquire)
    }
}


#[derive(Debug)]
#[must_use = "dropping `RcuSyncHandle` will cause memory leak"]
pub struct RcuSyncHandle<T: Send> {
    data: *mut T
}

unsafe impl<T: Send> Send for RcuSyncHandle<T> {}

impl<T: Send> RcuSyncHandle<T> {
    pub async fn get(self) -> T {
        *self.get_boxed().await
    }

    pub async fn get_boxed(self) -> Box<T> {
        Rcu::<T>::synchronize_rcu().await;
        unsafe { Box::from_raw(self.data) }
    }
}

/// An immutable and `!Send` reference to the RCU data.
#[derive(Debug, Clone, Copy)]
pub struct RcuReference<T> {
    data: *const T,
}

unsafe impl<T: Sync> Sync for RcuReference<T> {}
// impl<T> !Send for RcuReference<T> {}

impl<T> RcuReference<T> {
    /// Make a new `RcuReference` from a component of the referenced data.
    pub fn map<U, F>(this: RcuReference<T>, f: impl FnOnce(&T) -> &U) -> RcuReference<U> {
        RcuReference {
            data: f(&*this) as *const U,
        }
    }

    /// Get the inner raw pointer
    pub fn into_inner(this: RcuReference<T>) -> *const T {
        this.data
    }
}

impl<T> Deref for RcuReference<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}