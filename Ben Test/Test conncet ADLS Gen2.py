# Databricks notebook source
df = spark.read.csv('abfss://raw@azrdevddpcn01adls.dfs.core.chinacloudapi.cn/BIHubUpload/test.csv')

display(df)
