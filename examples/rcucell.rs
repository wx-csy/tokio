use rand::random;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Rcu;
use rcu_cell::RcuCell;

#[derive(Clone, Debug)]
struct TestStruct(i32);

impl Drop for TestStruct {
    fn drop(&mut self) {
        // prevent compiler optimization
        *std::convert::identity(&mut self.0) = -1;
    }
}

const NUM_ITER: usize = 10000000;
const NUM_TASK: usize = 32;
const WRITE_RATIO: u32 = 1000;
const YIELD_RATIO: usize = 2000;

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<(), Box<dyn Error>> {
    let test_data = Arc::new(unsafe { RcuCell::new(Some(TestStruct(0))) });
    let handles: Vec<_> = (0..NUM_TASK)
        .map(|_| {
            let test_data = test_data.clone();
            tokio::spawn(async move {
                let mut update_cnt = 0;
                for j in 0..NUM_ITER {
                    if random::<u32>() % WRITE_RATIO == 0 {
                        let mut lock = loop {
                            if let Some(lk) = test_data.try_lock() {
                                break lk;
                            }
                        };
                        let new = lock.as_ref().cloned().map(|mut x| { x.0 += 1; x });
                        lock.update(new);
                        update_cnt += 1;
                    } else {
                        let data = test_data.read().unwrap().0;
                        assert_ne!(data, -1);
                    }
                    if j & YIELD_RATIO == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                update_cnt
            })
        })
        .collect();

    let mut total = 0;
    for handle in handles {
        total += handle.await.unwrap();
    }
    eprintln!("Total updated: {}", total);
    assert_eq!(total, test_data.read().unwrap().0);

    Ok(())
}
