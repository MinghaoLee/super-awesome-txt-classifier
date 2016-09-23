# super-awesome-txt-classifier

[![Join the chat at https://gitter.im/super-awesome-txt-classifier/Lobby](https://badges.gitter.im/super-awesome-txt-classifier/Lobby.svg)](https://gitter.im/super-awesome-txt-classifier/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/snakes-in-the-box/super-awesome-txt-classifier.svg?branch=master)](https://travis-ci.org/snakes-in-the-box/super-awesome-txt-classifier)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/bf74add8c41e430ea98c217918ccd3bd)](https://www.codacy.com/app/snakes-in-the-box/super-awesome-txt-classifier?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=snakes-in-the-box/super-awesome-txt-classifier&amp;utm_campaign=Badge_Grade)  

#Usage

####Build for spark
```sbt assembly```
####Run in local machine
```./spark-submit --class "org.snakesinthebox.preprocessing.Main" --master local[4] <path to jar> --files <path to config>```
