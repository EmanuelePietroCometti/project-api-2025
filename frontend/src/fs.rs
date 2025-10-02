use fs::MyFS;
use polyfuse::Session;
use anyhow::Result;
use tokio::signal;



use polyfuse::{
    request::Request,
    reply,
    Filesystem,
};
use anyhow::Result;

pub struct MyFS;

#[async_trait::async_trait]
impl Filesystem for MyFS {
    async fn getattr(&self, req: &Request, reply: reply::ReplyAttr) -> Result<()> {
        reply.error(libc::ENOENT);
        Ok(())
    }
}
