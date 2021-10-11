package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

case class Default(column: String, default: String)
object Default {
  implicit val encoder: Encoder[Default] = deriveEncoder[Default]
  implicit val decoder: Decoder[Default] = deriveDecoder[Default]
}
