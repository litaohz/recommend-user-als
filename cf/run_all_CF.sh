#!/bin/bash

#基于评论/关注/关注和评论的协同过滤

bash -x run_newitemsimilarity.sh CommentCF.conf
bash -x run_newitemsimilarity.sh FollowCF.conf
bash -x run_newitemsimilarity.sh FollowCommentCF.conf
