pipeline {
  agent {
    docker {
      label 'docker-build-node'
      image 'golang:latest'
    }

  }
  stages {
    stage('') {
      steps {
        sh 'build/build.sh'
      }
    }

  }
}
