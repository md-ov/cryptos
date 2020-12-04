package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants

object Decision {
  def run(upDownPredict: Double, sizePredict: Double, size: Int, deltaPercentage: Double, thresholdForPositive: Double, thresholdForNegative: Double) = {
    println("--- Decision ---")

    if (deltaPercentage < constants.relativeMinDelta && size > 35) {
      println(s"size $size > 35 is good and delta value is small : $deltaPercentage < ${constants.relativeMinDelta}")
      val marginSize: Double = (sizePredict - size) / sizePredict
      println(s"margin size : $sizePredict - $size / $sizePredict = $marginSize")
      if (upDownPredict > thresholdForPositive) {
        println(s"prediction up : $upDownPredict > $thresholdForPositive")
        val errorRatio: Double = sizePredictScoresForUpSegment.find(x => x._1._1 <= sizePredict && sizePredict <= x._1._2).get._2
        if (errorRatio <= marginSize) {
          println(s"sizePredict [$sizePredict] for up segment may be good : $errorRatio (error Ratio) <= $marginSize (margin Size)")
          println("BUY")
        } else {
          println(s"sizePredict ($sizePredict) with error ratio for up segment too big and may not be good : $errorRatio (error Ratio) > $marginSize (margin Size)")
          println("HOLD")
        }
      } else if (upDownPredict < thresholdForNegative) {
        println(s"prediction down : $upDownPredict < $thresholdForNegative")
        val errorRatio: Double = sizePredictScoresForDownSegment.find(x => x._1._1 <= sizePredict && sizePredict <= x._1._2).get._2
        if (errorRatio <= marginSize) {
          println(s"sizePredict [$sizePredict] for down segment may be good : $errorRatio (error Ratio) <= $marginSize (margin Size)")
          println("SELL")
        } else {
          println(s"sizePredict ($sizePredict) with error ratio for down segment too big and may not be good : $errorRatio (error Ratio) >$marginSize (margin Size)")
          println("HOLD")
        }
      } else {
        println("HOLD")
      }
    } else {
      println(s"size $size <= 35 or delta value is big : $deltaPercentage > ${constants.relativeMinDelta}")
      println("HOLD")
    }
  }

  //meilleur score : 280 - 400
  val sizePredictScoresForDownSegment = Map(
    (0, 10) -> 2.404775417242381,
    (10, 20) -> 1.1138944835828595,
    (20, 30) -> 0.8858489899711105,
    (30, 40) -> 0.7390728473256906,
    (40, 50) -> 0.9029530118156314,
    (50, 60) -> 0.7833815821440941,
    (60, 70) -> 0.615209945073797,
    (70, 80) -> 0.5391362096542394,
    (80, 90) -> 0.5240047132795698,
    (90, 100) -> 0.38779949230514166,
    (100, 110) -> 0.2820626346005575,
    (110, 120) -> 0.43690808252859553,
    (120, 130) -> 0.48344458675473284,
    (130, 140) -> 0.403221527954004,
    (140, 150) -> 0.31686615436155807,
    (150, 160) -> 0.3135455732437548,
    (160, 170) -> 0.2504838949415937,
    (170, 180) -> 0.26571362405104104,
    (180, 190) -> 0.24592894749332267,
    (190, 200) -> 0.21001739990439333,
    (200, 210) -> 0.4194763747165379,
    (210, 220) -> 0.3997621525700088,
    (220, 230) -> 0.374114847245192,
    (230, 240) -> 0.3701342001392337,
    (240, 250) -> 0.3721245236922128,
    (250, 260) -> 0.34378132123570215,
    (260, 270) -> 0.13941995868537274,
    (270, 280) -> 0.08471128842690989,
    (280, 290) -> 0.07973547954446208,
    (290, 300) -> 0.07973547954446208,
    (300, 310) -> 0.07973547954446208,
    (310, 320) -> 0.08222338398568599,
    (320, 330) -> 0.07973547954446208,
    (330, 340) -> 0.0776565951357677,
    (340, 350) -> 0.07936470661189511,
    (350, 360) -> 0.07924295589166028,
    (360, 370) -> 0.07927774181172738,
    (370, 380) -> 0.07941688549199576,
    (380, 390) -> 0.07941688549199576,
    (390, 400) -> 0.08557874586618927,
    (400, 410) -> 0.09174121184839357,
    (410, 420) -> 0.018721533402038585
  )

  //meilleur score : 430-450
  val sizePredictScoresForUpSegment = Map(
    (0, 10) -> 1.92191807797,
    (10, 20) -> 1.11938807443,
    (20, 30) -> 0.78466136808,
    (30, 40) -> 0.63546834541,
    (40, 50) -> 0.72120658699,
    (50, 60) -> 0.62388845053,
    (60, 70) -> 0.58170767698,
    (70, 80) -> 0.48264562957,
    (80, 90) -> 0.42102726662,
    (90, 100) -> 0.350870724054,
    (100, 110) -> 0.334804492351,
    (110, 120) -> 0.49909341955,
    (120, 130) -> 0.53828783367,
    (130, 140) -> 0.4748139091,
    (140, 150) -> 0.40679589142,
    (150, 160) -> 0.326140171465,
    (160, 170) -> 0.29145244757,
    (170, 180) -> 0.29236104857,
    (180, 190) -> 0.29444069789,
    (190, 200) -> 0.281441830841,
    (200, 210) -> 0.32198942818,
    (210, 220) -> 0.31900395039,
    (220, 230) -> 0.32167711112,
    (230, 240) -> 0.32380314702,
    (240, 250) -> 0.32357640721,
    (250, 260) -> 0.32318219541,
    (260, 270) -> 0.320705102663,
    (270, 280) -> 0.32349669400,
    (280, 290) -> 0.31922290563,
    (290, 300) -> 0.282914705066,
    (300, 310) -> 0.271226643962,
    (310, 320) -> 0.271226643962,
    (320, 330) -> 0.271304272612,
    (330, 340) -> 0.224781947607,
    (340, 350) -> 0.18531141591,
    (350, 360) -> 0.18531141591,
    (360, 370) -> 0.18531141591,
    (370, 380) -> 0.18531141591,
    (380, 390) -> 0.18531141591,
    (390, 400) -> 0.18531141591,
    (400, 410) -> 0.177207213494,
    (410, 420) -> 0.14920574088,
    (420, 430) -> 0.142018495788,
    (430, 440) -> 0.132435502328,
    (440, 450) -> 0.132435502328,
    (450, 460) -> 0.141160091120,
    (460, 470) -> 0.171696151893,
    (470, 480) -> 0.194089920510,
    (480, 490) -> 0.29859417405,
    (490, 500) -> 0.29859417405,
    (500, 510) -> 0.29859417405,
    (510, 520) -> 0.29859417405,
    (520, 530) -> 0.298594174055
  )
}
