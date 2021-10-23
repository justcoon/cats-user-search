package com.jc.user.search

import cats.effect._

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    if (args.headOption.map(_ == "--do-it").getOrElse(false))
      IO.println("I did it!").as(ExitCode.Success)
    else
      IO.println("Didn't do it").as(ExitCode(-1))
}
