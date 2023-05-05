#!groovy

@Library('pipelib')
import org.veupathdb.lib.Builder

node('centos8') {
  sh "env"

  def builder = new Builder(this)

  builder.gitClone()
  builder.buildContainers([
    [ name: 'tool-eda-file-dumper' ]
  ])
}