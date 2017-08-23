package com.carrefour.ingestion.commons.util

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

import scala.util.Try

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.spark.input.PortableDataStream
import java.util.regex.Pattern

object ExtractionUtils {

  /**
    * Type of archive (tar, zip)
    */
  sealed trait ArchiveType

  case object ZipArchive extends ArchiveType

  case object TarArchive extends ArchiveType

  /**
    * Type of compression (gz)
    */
  sealed trait CompressionType

  case object GzipCompression extends CompressionType

  /**
    * Extracts the files from the given tar.gz stream. It returns the content of each tar entry with its name.
    * Only files are considered, directories will be ignored.
    *
    * @param ps The tar.gz stream
    * @param n  Number of bytes to read in each read operation
    * @see http://stackoverflow.com/a/36707257
    */
  def extractArchiveFiles(ps: PortableDataStream, archiveType: ArchiveType, compressionType: Option[CompressionType] = None, fileNamePattern: Option[Pattern] = None, n: Int = 1024): Try[Seq[(String, Array[Byte])]] = Try {
    val uncompressedInputStream = compressionType match {
      case Some(GzipCompression) => new GzipCompressorInputStream(ps.open)
      case Some(c) => throw new IllegalArgumentException(s"Unsupported compression type $c")
      case None => ps.open()
    }
    val archive = archiveType match {
      case ZipArchive => new ZipArchiveInputStream(uncompressedInputStream)
      case TarArchive => new TarArchiveInputStream(uncompressedInputStream)
      case a => throw new IllegalArgumentException(s"Unsupported archive type $a")
    }
    val buffer = Array.fill[Byte](n)(-1)
    Stream.continually(Option(archive.getNextEntry))
      // Read until next entry is null
      .takeWhile(_.isDefined)
      .flatten
      // Drop directories
      .filter(!_.isDirectory)
      .filter { e =>
        fileNamePattern match {
          case Some(pattern) => pattern.matcher(e.getName).matches
          case None => true
        }
      }
      .map(e => {
        (e.getName,
          Stream.continually {
            // Read n bytes
            val i = archive.read(buffer, 0, n)
            (i, buffer.take(i))
          }
            // Take as long as we've read something
            .takeWhile(_._1 > 0)
            .flatMap(_._2)
            .toArray)
      })
  }

  def decode(bytes: Array[Byte], charset: Charset = StandardCharsets.UTF_8): String = new String(bytes, charset)

}