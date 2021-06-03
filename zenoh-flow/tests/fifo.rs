//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

// use zenoh_flow::fifo::{new_fifo, ZFFifoReceiver, ZFFifoSender};
// use zenoh_flow::stream::{ZFFIFOError, ZFLinkReceiverTrait, ZFLinkSenderTrait};

// async fn same_task_simple() {
//     let size = 2;
//     let id = 0;
//     let (sender, receiver) = new_fifo::<u8>(size, id);

//     let mut d: u8 = 0;
//     // Add the first element
//     let res = sender.send(d).await;
//     assert_eq!(res, Ok(()));
//     let res = receiver.recv().await;
//     assert_eq!(res, Ok((0u8, 0usize)));

//     // Add the second element
//     d = d + 1;
//     let res = sender.send(d).await;
//     assert_eq!(res, Ok(()));
//     let res = receiver.recv().await;
//     assert_eq!(res, Ok((1u8, 0usize)));
// }

// async fn recv_task_simple(receiver: ZFFifoReceiver<u8>) {
//     let res = receiver.recv().await;
//     assert_eq!(res, Ok((0u8, 0usize)));

//     let res = receiver.recv().await;
//     assert_eq!(res, Ok((1u8, 0usize)));

//     let res = receiver.recv().await;
//     assert_eq!(res, Ok((2u8, 0usize)));
// }

// async fn send_task_simple(sender: ZFFifoSender<u8>) {
//     let mut d: u8 = 0;
//     // Add the first element
//     let res = sender.send(d).await;
//     assert_eq!(res, Ok(()));
//     // Add the second element
//     d = d + 1;
//     let res = sender.send(d).await;
//     assert_eq!(res, Ok(()));

//     // Add the 3rd element
//     d = d + 1;
//     let res = sender.send(d).await;
//     assert_eq!(res, Ok(()));
// }

// async fn recv_task_more(receiver: ZFFifoReceiver<u8>) {
//     for n in 0u8..255u8 {
//         let res = receiver.recv().await;
//         assert_eq!(res, Ok((n, 0usize)));
//     }
// }

// async fn send_task_more(sender: ZFFifoSender<u8>) {
//     for n in 0u8..255u8 {
//         let res = sender.send(n).await;
//         assert_eq!(res, Ok(()));
//     }
// }

// #[test]
// fn ordered_fifo_simple_sync() {
//     let size = 2;
//     let id = 0;
//     let (sender, receiver) = new_fifo::<u8>(size, id);

//     let mut d: u8 = 0;
//     // Add the first element
//     let res = sender.try_send(d);
//     assert_eq!(res, Ok(()));
//     let res = receiver.try_recv();
//     assert_eq!(res, Ok((0u8, 0usize)));

//     // Add the second element
//     d = d + 1;
//     let res = sender.try_send(d);
//     assert_eq!(res, Ok(()));
//     let res = receiver.try_recv();
//     assert_eq!(res, Ok((1u8, 0usize)));

//     // Verify that the queue is empty
//     //assert_eq!(queue.len(), 0);
// }

// #[test]
// fn ordered_fifo_simple_async() {
//     async_std::task::block_on(async move { same_task_simple().await })
// }

// #[test]
// fn ordered_fifo_simple_two_task_async() {
//     let size = 2;
//     let id = 0;
//     let (sender, receiver) = new_fifo::<u8>(size, id);

//     let h1 = async_std::task::spawn(async move { send_task_simple(sender).await });

//     let h2 = async_std::task::spawn(async move { recv_task_simple(receiver).await });

//     async_std::task::block_on(async move {
//         h1.await;
//         h2.await;
//     })
// }

// #[test]
// fn ordered_fifo_more_two_task_async() {
//     let size = 20;
//     let id = 0;
//     let (sender, receiver) = new_fifo::<u8>(size, id);

//     let h1 = async_std::task::spawn(async move { send_task_more(sender).await });

//     let h2 = async_std::task::spawn(async move { recv_task_more(receiver).await });

//     async_std::task::block_on(async move {
//         h1.await;
//         h2.await;
//     })
// }
