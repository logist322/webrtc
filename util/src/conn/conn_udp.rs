use tokio::net::UdpSocket;
use crate::PACKETS;

use super::*;

#[async_trait]
impl Conn for UdpSocket {
    async fn connect(&self, addr: SocketAddr) -> Result<()> {
        Ok(self.connect(addr).await?)
    }

    async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        Ok(self.recv(buf).await?)
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let b = self.recv_from(buf).await?;

        unsafe { PACKETS.lock().unwrap().insert(buf[..b.0].to_vec()) };

        Ok(b)
    }

    async fn send(&self, buf: &[u8]) -> Result<usize> {
        Ok(self.send(buf).await?)
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> Result<usize> {
        Ok(self.send_to(buf, target).await?)
    }

    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.local_addr()?)
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
