pipeline {
  agent {
    docker {
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