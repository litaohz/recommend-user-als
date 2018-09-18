package com.netease.music.recommend.scala.feedflow.utils

object CaseClassObject {

  case class ControversialArtistidForMarking(controversialArtistidForMarking:Long)

  case class ControversialEventFromKeywordContent(eventId:Long, artistsFromKeywordContent:String)

  case class ControversialEventFromKeywordCreator(creatorIdFromNickname:Long, artistsFromKeywordCreator:String)
}
