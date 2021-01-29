// scalastyle:off
/*
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:on
package com.github.datasource.s3

import java.net.URI

import com.amazonaws.services.s3.AmazonS3URI

/** Provides methods related to S3 URI manipulation.
 *
 */
object S3URI {
  /** return a real AmazonS3URI for the given input string.
   * This does the translation for s3, s3a, or s3n URLs.
   *
   * @param location the endpoint to connect to, includes the
   *                 "s3://", "s3a://", or "s3n://"
   * @return the AmazonS3URI object.
   */
  def toAmazonS3URI(
    location: String): AmazonS3URI = {
    val uri = new URI(location)
    val uriScheme = uri.getScheme
    uriScheme match {
      case "s3" =>
        new AmazonS3URI(uri)
      case "s3a" | "s3n" =>
        new AmazonS3URI(new URI("s3", uri.getUserInfo, uri.getHost, uri.getPort, uri.getPath,
          uri.getQuery, uri.getFragment))
      case other =>
        throw new IllegalArgumentException(
          s"Unrecognized scheme $other; expected s3, or s3a or s3n")
    }
  }
}
