use std::io::{self, Read, Seek, SeekFrom};

use miniz_oxide::inflate::{
    TINFLStatus,
    core::{self, BlockBoundaryState, DecompressorOxide, inflate_flags},
};

const INPUT_BUF_SIZE: usize = 128 * 1024;
const OUTPUT_BUF_SIZE: usize = 64 * 1024;
const WINDOW_SIZE: usize = core::TINFL_LZ_DICT_SIZE;

#[derive(Debug, Clone)]
pub(crate) struct DeflateIndex {
    checkpoints: Vec<DeflateCheckpoint>,
    uncompressed_len: u64,
}

#[derive(Clone)]
struct DeflateCheckpoint {
    compressed_offset: u64,
    output_start: u64,
    output_boundary: u64,
    state: BlockBoundaryState,
    window: Vec<u8>,
}

impl std::fmt::Debug for DeflateCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeflateCheckpoint")
            .field("compressed_offset", &self.compressed_offset)
            .field("output_start", &self.output_start)
            .field("output_boundary", &self.output_boundary)
            .field("state_num_bits", &self.state.num_bits)
            .field("window_len", &self.window.len())
            .finish()
    }
}

impl DeflateIndex {
    pub(crate) fn build<R: Read>(
        reader: R,
        expected_uncompressed_len: u64,
        interval_bytes: u64,
    ) -> io::Result<Self> {
        let mut builder = DeflateIndexBuilder::new(reader, interval_bytes);
        builder.run()?;

        if builder.output_dec != expected_uncompressed_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "deflated ZIP member length mismatch: ZIP directory says {} bytes, miniz_oxide produced {} bytes",
                    expected_uncompressed_len, builder.output_dec
                ),
            ));
        }

        Ok(Self {
            checkpoints: builder.checkpoints,
            uncompressed_len: builder.output_dec,
        })
    }

    pub(crate) fn checkpoint_count(&self) -> usize {
        self.checkpoints.len()
    }

    pub(crate) fn window_bytes(&self) -> usize {
        self.checkpoints
            .iter()
            .map(|checkpoint| checkpoint.window.len())
            .sum()
    }

    pub(crate) fn indexed_reader<R: Read + Seek>(
        &self,
        reader: R,
        offset: u64,
    ) -> io::Result<DeflateIndexedReader<'_, R>> {
        if offset > self.uncompressed_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "deflate seek offset {} exceeds uncompressed length {}",
                    offset, self.uncompressed_len
                ),
            ));
        }

        let mut reader = DeflateIndexedReader::new(self, reader)?;
        reader.seek_to_offset(offset)?;
        Ok(reader)
    }

    pub(crate) fn read_exact_at<R: Read + Seek>(
        &self,
        reader: R,
        offset: u64,
        buf: &mut [u8],
    ) -> io::Result<()> {
        let end = offset
            .checked_add(u64::try_from(buf.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "buffer length does not fit in u64",
                )
            })?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "deflate read offset overflow")
            })?;
        if end > self.uncompressed_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "indexed deflate read exceeds uncompressed length",
            ));
        }

        let mut reader = self.indexed_reader(reader, offset)?;
        reader.read_exact(buf)
    }
}

struct DeflateIndexBuilder<R> {
    reader: R,
    decompressor: Box<DecompressorOxide>,
    input: Vec<u8>,
    input_offset: usize,
    input_size: usize,
    input_pos: u64,
    output: Vec<u8>,
    output_dec: u64,
    interval_bytes: u64,
    last_checkpoint_boundary: u64,
    checkpoints: Vec<DeflateCheckpoint>,
    done: bool,
}

impl<R: Read> DeflateIndexBuilder<R> {
    fn new(reader: R, interval_bytes: u64) -> Self {
        Self {
            reader,
            decompressor: Box::new(DecompressorOxide::new()),
            input: vec![0; INPUT_BUF_SIZE],
            input_offset: 0,
            input_size: 0,
            input_pos: 0,
            output: vec![0; OUTPUT_BUF_SIZE],
            output_dec: 0,
            interval_bytes: interval_bytes.max(1),
            last_checkpoint_boundary: 0,
            checkpoints: Vec::new(),
            done: false,
        }
    }

    fn run(&mut self) -> io::Result<()> {
        while !self.done {
            self.make_progress()?;
        }
        Ok(())
    }

    fn refill_input(&mut self) -> io::Result<()> {
        if self.input_offset >= self.input_size {
            self.input_offset = 0;
            self.input_size = self.reader.read(&mut self.input)?;
        }
        Ok(())
    }

    fn make_progress(&mut self) -> io::Result<()> {
        self.refill_input()?;
        let flags = inflate_flags::TINFL_FLAG_STOP_ON_BLOCK_BOUNDARY
            | if self.input_offset < self.input_size {
                inflate_flags::TINFL_FLAG_HAS_MORE_INPUT
            } else {
                0
            };

        let (status, consumed, produced) = core::decompress(
            &mut self.decompressor,
            &self.input[self.input_offset..self.input_size],
            &mut self.output,
            (self.output_dec % OUTPUT_BUF_SIZE as u64) as usize,
            flags,
        );
        self.input_offset += consumed;
        self.input_pos = self.input_pos.checked_add(consumed as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "compressed byte count overflow")
        })?;
        self.output_dec = self
            .output_dec
            .checked_add(produced as u64)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "uncompressed byte count overflow",
                )
            })?;

        match status {
            TINFLStatus::Done => {
                self.done = true;
                Ok(())
            },
            TINFLStatus::HasMoreOutput | TINFLStatus::NeedsMoreInput => Ok(()),
            TINFLStatus::BlockBoundary => self.maybe_create_checkpoint(),
            TINFLStatus::FailedCannotMakeProgress => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "deflate stream ended before miniz_oxide reached the stream end",
            )),
            _ => Err(inflate_error(status)),
        }
    }

    fn maybe_create_checkpoint(&mut self) -> io::Result<()> {
        if self.output_dec < WINDOW_SIZE as u64 {
            return Ok(());
        }
        if self.output_dec - self.last_checkpoint_boundary < self.interval_bytes {
            return Ok(());
        }

        let state = self
            .decompressor
            .block_boundary_state()
            .ok_or_else(|| io::Error::other("missing miniz_oxide block boundary state"))?;
        let output_start = self.output_dec - WINDOW_SIZE as u64;
        let window = snapshot_window(&self.output, output_start);

        self.checkpoints.push(DeflateCheckpoint {
            compressed_offset: self.input_pos,
            output_start,
            output_boundary: self.output_dec,
            state,
            window,
        });
        self.last_checkpoint_boundary = self.output_dec;
        Ok(())
    }
}

pub(crate) struct DeflateIndexedReader<'a, R> {
    index: &'a DeflateIndex,
    reader: R,
    decompressor: Box<DecompressorOxide>,
    input: Vec<u8>,
    input_offset: usize,
    input_size: usize,
    input_pos: u64,
    output: Vec<u8>,
    output_dec: u64,
    output_ret: u64,
    done: bool,
}

impl<'a, R: Read + Seek> DeflateIndexedReader<'a, R> {
    fn new(index: &'a DeflateIndex, mut reader: R) -> io::Result<Self> {
        reader.seek(SeekFrom::Start(0))?;
        Ok(Self {
            index,
            reader,
            decompressor: Box::new(DecompressorOxide::new()),
            input: vec![0; INPUT_BUF_SIZE],
            input_offset: 0,
            input_size: 0,
            input_pos: 0,
            output: vec![0; OUTPUT_BUF_SIZE],
            output_dec: 0,
            output_ret: 0,
            done: false,
        })
    }

    fn restore_checkpoint(&mut self, checkpoint: &DeflateCheckpoint) -> io::Result<()> {
        self.reader
            .seek(SeekFrom::Start(checkpoint.compressed_offset))?;
        self.decompressor = Box::new(DecompressorOxide::from_block_boundary_state(
            &checkpoint.state,
        ));
        self.input_offset = 0;
        self.input_size = 0;
        self.input_pos = checkpoint.compressed_offset;
        self.output_ret = checkpoint.output_start;
        self.output_dec = checkpoint.output_boundary;
        self.done = false;

        for (idx, byte) in checkpoint.window.iter().copied().enumerate() {
            let absolute_pos = checkpoint.output_start + idx as u64;
            self.output[(absolute_pos % OUTPUT_BUF_SIZE as u64) as usize] = byte;
        }
        Ok(())
    }

    fn checkpoint_for_offset(&self, offset: u64) -> Option<&DeflateCheckpoint> {
        let partition = self
            .index
            .checkpoints
            .partition_point(|checkpoint| checkpoint.output_start <= offset);
        if partition == 0 {
            None
        } else {
            Some(&self.index.checkpoints[partition - 1])
        }
    }

    fn seek_to_offset(&mut self, offset: u64) -> io::Result<()> {
        if let Some(checkpoint) = self.checkpoint_for_offset(offset).cloned() {
            self.restore_checkpoint(&checkpoint)?;
        }
        self.skip(offset - self.output_ret)
    }

    fn refill_input(&mut self) -> io::Result<()> {
        if self.input_offset >= self.input_size {
            self.input_offset = 0;
            self.input_size = self.reader.read(&mut self.input)?;
        }
        Ok(())
    }

    fn has_output(&self) -> bool {
        self.output_dec != self.output_ret
    }

    fn make_progress(&mut self) -> io::Result<()> {
        self.refill_input()?;
        let flags = if self.input_offset < self.input_size {
            inflate_flags::TINFL_FLAG_HAS_MORE_INPUT
        } else {
            0
        };
        let (status, consumed, produced) = core::decompress(
            &mut self.decompressor,
            &self.input[self.input_offset..self.input_size],
            &mut self.output,
            (self.output_dec % OUTPUT_BUF_SIZE as u64) as usize,
            flags,
        );
        self.input_offset += consumed;
        self.input_pos = self.input_pos.checked_add(consumed as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "compressed byte count overflow")
        })?;
        self.output_dec = self
            .output_dec
            .checked_add(produced as u64)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "uncompressed byte count overflow",
                )
            })?;

        match status {
            TINFLStatus::Done => {
                self.done = true;
                Ok(())
            },
            TINFLStatus::HasMoreOutput | TINFLStatus::NeedsMoreInput => Ok(()),
            TINFLStatus::FailedCannotMakeProgress => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "deflate stream ended before miniz_oxide reached the stream end",
            )),
            _ => Err(inflate_error(status)),
        }
    }

    fn flush_output(&mut self, buf: &mut [u8]) -> usize {
        let available = (self.output_dec - self.output_ret) as usize;
        let copied = available.min(buf.len());
        let start = (self.output_ret % OUTPUT_BUF_SIZE as u64) as usize;
        let first = copied.min(OUTPUT_BUF_SIZE - start);
        buf[..first].copy_from_slice(&self.output[start..start + first]);
        if first < copied {
            buf[first..copied].copy_from_slice(&self.output[..copied - first]);
        }
        self.output_ret += copied as u64;
        copied
    }

    fn skip(&mut self, amount: u64) -> io::Result<()> {
        let mut remaining = amount;
        while remaining > 0 {
            while !self.has_output() && !self.done {
                self.make_progress()?;
            }

            let skipped = (self.output_dec - self.output_ret).min(remaining);
            self.output_ret += skipped;
            remaining -= skipped;
            if skipped == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "seek past end of deflate stream",
                ));
            }
        }
        Ok(())
    }
}

impl<R: Read + Seek> Read for DeflateIndexedReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        while !self.has_output() && !self.done {
            self.make_progress()?;
        }
        Ok(self.flush_output(buf))
    }
}

fn snapshot_window(output: &[u8], output_start: u64) -> Vec<u8> {
    let mut window = Vec::with_capacity(WINDOW_SIZE);
    for idx in 0..WINDOW_SIZE {
        let absolute_pos = output_start + idx as u64;
        window.push(output[(absolute_pos % OUTPUT_BUF_SIZE as u64) as usize]);
    }
    window
}

fn inflate_error(status: TINFLStatus) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("miniz_oxide inflate failed with status {status:?}"),
    )
}
