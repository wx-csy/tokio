use std::marker::PhantomData;
use std::ops::Deref;
use std::task::Poll;
use std::future::Future;

use crate::loom::sync::atomic::AtomicPtr;
use crate::loom::sync::atomic::Ordering;

/// An RCU pointer.
#[derive(Debug)]
pub struct Rcu<T: Send + 'static> {
    data: AtomicPtr<T>
}

unsafe impl <T: Send + 'static> Send for Rcu<T> {}
unsafe impl <T: Send + Sync + 'static> Sync for Rcu<T> {}

impl<T: Send + 'static> Drop for Rcu<T> {
    fn drop(&mut self) {
        unsafe { Box::from_raw(self.data.load(Ordering::Acquire)); }
    }
}

impl<T: Send + 'static> Rcu<T> {
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

    /// Read the current RCU pointer. The returned reference is immutable and `!Send`.
    pub fn read(&self) -> RcuReference<'_, T> {
        RcuReference {
            data: unsafe { &*(self.data.load(Ordering::Acquire) as *const T) } ,
            _phantom: PhantomData,
        }
    }

    /// Compare the current RCU pointer with `expect`, and if equal, update it with `new`, 
    /// return the synchronization handle. Otherwise, return the reference to the current pointer.
    /// The entire operation is guaranteed to be atomic.
    pub fn compare_update(&self, expect: RcuReference<'_, T>, new: T) -> Result<RcuSyncHandle<T>, RcuReference<'_, T>> {
        let ptr_new = Box::into_raw(Box::new(new));
        match self.data.compare_exchange(expect.as_ptr() as *mut T, ptr_new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(old) => {
                Ok(RcuSyncHandle {
                    data: old
                })
            },
            Err(old) => {
                std::mem::drop(unsafe { Box::from_raw(ptr_new) });
                Err(RcuReference {
                    data: unsafe { &*(old as *const T) },
                    _phantom: PhantomData,
                })
            },
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
        self.data.load(Ordering::Acquire)
    }
}


#[derive(Debug)]
pub struct RcuSyncHandle<T: Send + 'static> {
    data: *mut T
}

unsafe impl<T: Send + 'static> Send for RcuSyncHandle<T> {}

struct PointerWrapper<T: Send + 'static>(*mut T);
unsafe impl<T: Send + 'static> Send for PointerWrapper<T> {}

impl<T: Send + 'static> RcuSyncHandle<T> {
    async unsafe fn sync_and_get(data: PointerWrapper<T>) -> Box<T> {
        Rcu::<T>::synchronize_rcu().await;
        Box::from_raw(data.0)
    }

    pub async fn get(self) -> T {
        *self.get_boxed().await
    }

    pub async fn get_boxed(mut self) -> Box<T> {
        let ptr = PointerWrapper(std::mem::replace(&mut self.data, std::ptr::null_mut()));
        unsafe { Self::sync_and_get(ptr) }.await 
    }

    /// Do not run synchronization task and leak the old value.
    pub fn leak(self) {}
}

impl<T: Send + 'static> Drop for RcuSyncHandle<T> {
    fn drop(&mut self) {
        let ptr = std::mem::replace(&mut self.data, std::ptr::null_mut());
        if !ptr.is_null() {
            let wrapped_pointer = PointerWrapper(ptr);
            crate::spawn(async move { unsafe { Self::sync_and_get(wrapped_pointer) }.await });
        }
    }
}

/// An immutable and `!Send` reference to the RCU data.
#[derive(Debug, Clone, Copy)]
pub struct RcuReference<'a, T> {
    data: &'a T,
    _phantom: PhantomData<*const T>,
}

unsafe impl<T: Sync> Sync for RcuReference<'_, T> {}
// impl<T> !Send for RcuReference<T> {}

impl<'a, T> RcuReference<'a, T> {
    /// Make a new `RcuReference` from a component of the referenced data.
    pub fn map<U, F>(this: RcuReference<'a, T>, f: impl FnOnce(&T) -> &U) -> RcuReference<'a, U> {
        RcuReference {
            data: f(this.data),
            _phantom: PhantomData,
        }
    }

    /// Get the inner raw pointer
    pub fn as_ptr(&self) -> *const T {
        self.data as *const T
    }

    /// Convert to the inner raw pointer
    pub fn too(this: RcuReference<'a, T>) -> *const T {
        this.as_ptr()
    }
}

impl<T> Deref for RcuReference<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.data
    }
}