use std::marker::PhantomData;
use std::ptr;
use std::ops::Deref;
use std::task::Poll;
use std::future::Future;

use crate::loom::sync::atomic::AtomicPtr;
use crate::loom::sync::atomic::Ordering;

/// An RCU pointer.
///
/// The pointer may be `Null`.
#[derive(Debug)]
pub struct Rcu<T: Send + 'static> {
    // This is actually an atomic cell.
    data: AtomicPtr<T>,
    // NOTE: this marker has no consequences for variance, but is necessary
    // for dropck to understand that we logically own a `T`.
    _marker: PhantomData<T>,
}

unsafe impl <T: Send + 'static> Send for Rcu<T> {}
unsafe impl <T: Send + Sync + 'static> Sync for Rcu<T> {}

impl<T: Send + 'static> Drop for Rcu<T> {
    fn drop(&mut self) {
        // Move out potential value.
        self.take();
    }
}

impl<T: Send + 'static> Default for Rcu<T> {
    fn default() -> Self {
        Rcu {
            data: AtomicPtr::new(ptr::null_mut()),
            _marker: PhantomData,
        }
    }
}

unsafe fn ptr_to_option_box<T>(ptr: *mut T) -> Option<Box<T>> {
    if ptr.is_null() {
        None
    } else {
        Some(Box::from_raw(ptr))
    }
}

fn option_box_to_ptr<T>(x: Option<Box<T>>) -> *mut T {
    x.map_or(ptr::null_mut(), Box::into_raw)
}

impl<T: Send + 'static> Rcu<T> {
    /// Create a new RCU pointer.
    pub unsafe fn new(x: Option<Box<T>>) -> Rcu<T> {
        Rcu {
            data: AtomicPtr::new(option_box_to_ptr(x)),
            _marker: PhantomData,
        }
    }

    /// Take the contained data from `Rcu`.
    pub fn take(&mut self) -> Option<Box<T>> {
        self.replace(None)
    }

    /// Set the contained data with 'val'.
    pub fn set(&mut self, val: Option<Box<T>>) {
        self.replace(val);
    }

    /// Replace the contained data from `Rcu` with `val`.
    pub fn replace(&mut self, val: Option<Box<T>>) -> Option<Box<T>> {
        let ptr = std::mem::replace(self.data.get_mut(), option_box_to_ptr(val));
        // SAFETY: ptr is taken from self.data
        unsafe { ptr_to_option_box(ptr) }
    }


    /// Load the current RCU pointer as raw pointer.
    pub fn read_ptr(&self) -> *const T {
        self.data.load(Ordering::Acquire) as *const T
    }

    /// Load the current RCU pointer. The returned reference is immutable and `!Send`.
    pub fn read(&self) -> Option<RcuReference<'_, T>> {
        // SAFETY: the pointer is read from the current valid RCU cell
        unsafe { RcuReference::new(self.read_ptr()) }
    }

    /// Compare the current RCU pointer with `expect`, and if equal, update it with `new`, 
    /// return the synchronization handle. Otherwise, return the reference to the current pointer.
    /// The entire operation is guaranteed to be atomic.
    pub fn compare_update(&self, current: Option<RcuReference<'_, T>>, new: Option<Box<T>>) -> Result<RcuSyncHandle<T>, (Option<Box<T>>, Option<RcuReference<'_, T>>)> {
        let current_ptr = current.map_or(ptr::null(), RcuReference::into_inner) as *mut T;
        let new_ptr = option_box_to_ptr(new);
        match self.data.compare_exchange(current_ptr, new_ptr, Ordering::AcqRel, Ordering::Acquire) {
            Ok(old) => {
                Ok(RcuSyncHandle {
                    data: old
                })
            },
            Err(old) => {
                Err((
                    // SAFETY: `new_ptr` is from `new`.
                    unsafe { ptr_to_option_box(new_ptr) }, 
                    // SAFETY: `old` is obtained from `self.data`.
                    unsafe { RcuReference::new(old) }
                ))
            },
        }
    }


    /// Fetches the old value, applies a function to it, and returns the old value as `RcuSyncHandle`. 
    pub fn update_with(&self, mut f: impl FnMut(Option<&mut T>)) -> RcuSyncHandle<T> 
        where T: Clone
    {
        let mut current = self.read();
        let mut new = current.clone().map(|x| Box::new(x.cloned()));
        loop {
            f(new.as_mut().map(|x| x.as_mut()));
            let current_ptr = current.map_or(ptr::null(), RcuReference::into_inner) as *mut T;
            let new_ptr = option_box_to_ptr(new);
            match self.data.compare_exchange_weak(current_ptr, new_ptr, Ordering::AcqRel, Ordering::Acquire) {
                Ok(old) => {
                    return RcuSyncHandle { data: old };
                },
                Err(old) => {
                    // SAFETY: this is read from current ptr.
                    current = unsafe { RcuReference::new(old) };
                    // SAFETY: this is the original value.
                    new = unsafe { ptr_to_option_box(new_ptr) };
                    if old.is_null() {
                        new = None;
                    } else {
                        // SAFETY: old is read from current RCU.
                        let cloned_old = unsafe { (*old).clone() };
                        if let Some(x) = new.as_mut() {
                            *x.as_mut() = cloned_old;
                        } else {
                            new = Some(Box::new(cloned_old));
                        }
                    }
                }
            }
        }
    }

    /// Write the current RCU pointer with boxed value. Wait for all readers leaving the grace period,
    /// then return the old boxed value.
    pub fn write(&self, new: Option<Box<T>>) -> RcuSyncHandle<T> {
        // SAFETY: the written ptr is taken from `new.
        unsafe { self.write_ptr(option_box_to_ptr(new)) }
    }

    /// Write the current RCU pointer with raw pointer. The raw pointer must be either null, or point to some valid
    /// owned value.
    pub unsafe fn write_ptr(&self, ptr: *mut T) -> RcuSyncHandle<T> {
        RcuSyncHandle {
            data: self.data.swap(ptr, Ordering::AcqRel)
        }
    }

    /// Consume this `Rcu`. Return the inner boxed value.
    pub fn into_inner(mut self) -> Option<Box<T>> {
        self.take()
    }

    /// Consume this `Rcu`. Return the raw pointer for the inner value. The pointer may be `Null`.
    pub fn into_raw(mut self) -> *mut T {
        *self.data.get_mut()
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

}


#[derive(Debug)]
pub struct RcuSyncHandle<T: Send + 'static> {
    data: *mut T
}

unsafe impl<T: Send + 'static> Sync for RcuSyncHandle<T> {}

struct PointerWrapper<T: Send + 'static>(*mut T);
unsafe impl<T: Send + 'static> Send for PointerWrapper<T> {}

impl<T: Send + 'static> RcuSyncHandle<T> {
    async unsafe fn sync_and_get(data: PointerWrapper<T>) -> Box<T> {
        Rcu::<T>::synchronize_rcu().await;
        Box::from_raw(data.0)
    }

    pub fn as_ref(&self) -> Option<&T> {
        let ptr = self.data;
        if ptr.is_null() {
            None
        } else {
            // SAFETY: the ptr is valid since it is not null.
            Some( unsafe { &*(ptr as *const T) })
        }
    }

    /// Return `None` is the ptr is null, else return `Some(self)`.
    pub fn test(self) -> Option<RcuSyncHandle<T>> {
        if self.data.is_null() {
            None
        } else {
            Some(self)
        }
    }

    /// Get the contained value as `Option<Box<T>>`.
    pub async fn get(mut self) -> Option<Box<T>> {
        let raw_ptr = std::mem::replace(&mut self.data, std::ptr::null_mut());
        if raw_ptr.is_null() {
            None
        } else {
            Some(unsafe { Self::sync_and_get(PointerWrapper(raw_ptr)) }.await) 
        }
    }

    pub fn as_ptr(&self) -> *mut T {
        self.data
    }

    /// Get the contained value as `Option<Box<T>>`, without performing any synchronization.
    pub unsafe fn get_unchecked(self) -> Option<Box<T>> {
        ptr_to_option_box(self.data)
    }

    /// Intentionally leak the value.
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
    data: *const T,
    _marker: PhantomData<&'a T>,
}

unsafe impl<T: Sync> Sync for RcuReference<'_, T> {}
// impl<T> !Send for RcuReference<T> {}

impl<'a, T> RcuReference<'a, T> {
    unsafe fn new(x: *const T) -> Option<RcuReference<'a, T>> {
        if x.is_null() {
            None 
        } else {
            RcuReference {
                data: x,
                _marker: PhantomData,
            }.into()
        }
    }

    /// Make a new `RcuReference` from a component of the referenced data.
    pub fn map<U, F>(this: RcuReference<'a, T>, f: impl FnOnce(&T) -> &U) -> RcuReference<'a, U> {
        RcuReference {
            data: f(this.deref()) as *const U,
            _marker: PhantomData,
        }
    }

    /// Copy the referenced data.
    pub fn copied(self) -> T
        where T: Copy
    {
        *self
    }

    /// Clone the referenced data.
    pub fn cloned(self) -> T
        where T: Clone
    {
        (*self).clone()
    }

    /// Get the inner raw pointer
    pub fn as_ptr(&self) -> *const T {
        self.data as *const T
    }

    /// Convert into raw pointer
    pub fn into_inner(self) -> *const T {
        self.as_ptr()
    }
}

impl<T> Deref for RcuReference<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.data }
    }
}