use rand::random;
use std::error::Error;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug)]
struct TestStruct(i32);

impl Drop for TestStruct {
    fn drop(&mut self) {
        // prevent compiler optimization
        *std::convert::identity(&mut self.0) = -1;
    }
}

const NUM_ITER: usize = 1000000;
const NUM_TASK: usize = 32;
const WRITE_RATIO: u32 = 10000;
const YIELD_RATIO: usize = 2000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let test_data = Arc::new(RwLock::new(TestStruct(3)));
    let handles: Vec<_> = (0..NUM_TASK)
        .map(|_| {
            let test_data = test_data.clone();
            tokio::spawn(async move {
                for j in 0..NUM_ITER {
                    if random::<u32>() % WRITE_RATIO == 0 {
                        test_data.write().unwrap().0 += 1;
                    } else {
                        let data = { test_data.read().unwrap().0 };
                        assert_ne!(data, -1);
                    }
                    if j % YIELD_RATIO == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    eprintln!("Total updated: {}", test_data.read().unwrap().0);

    Ok(())
}
