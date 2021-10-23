pipeline {
    agent any

    stages {
        stage('Setup'){
            steps{
                sh 'rm -rf spark-stuff 2> /dev/null'
                sh 'curl -o spark.tgz https://dlcdn.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz'
                sh 'tar zxf spark.tgz'
                sh "mv -f 'spark-3.2.0-bin-hadoop3.2' spark-stuff "
                dir ('spark-stuff'){
                    sh 'curl -o mysql.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar'
                    sh 'ls'
                    sh 'mv -f mysql.jar jars/mysql.jar'
                    sh 'curl -o spark-streaming-kinesis-asl-assembly_2.12-3.3.0.jar http://ss-utopia-build-resources.s3.amazonaws.com/jars/spark-streaming-kinesis-asl-assembly_2.12-3.3.0.jar'
                    sh 'mv spark-streaming-kinesis-asl-assembly_2.12-3.3.0.jar jars/spark-streaming-kinesis-asl-assembly_2.12-3.3.0.jar'
                }
            }
        }
        stage('Pull') {
            steps {
                dir('spark-stuff'){
                    sh 'ls'
                    dir('ss-utopia-spark'){
                        git branch: 'feature/jenkins', url: 'https://github.com/byte-crunchers/ss-utopia-spark'
                    }
                    sh 'mv -f ss-utopia-spark/kubernetes/dockerfiles/spark/bindings/python/Dockerfile kubernetes/dockerfiles/spark/bindings/python/Dockerfile'
                    //sh 'rsync -a ./ss-utopia-spark/kubernetes ./spark/kubernetes'
                }
            }
        }
        
        stage('Build') {
            steps {
                dir('spark-stuff'){
                    sh './bin/docker-image-tool.sh -r ss-utopia-spark -t latest -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build'
                }
            }
        }
        stage('Push') {
            steps {
                dir('spark-stuff'){
                    sh 'rm -f ~/.dockercfg ~/.docker/config.json || true 2> /dev/null'
                    script{
                        docker.withRegistry("https://422288715120.dkr.ecr.us-east-1.amazonaws.com", "ecr:us-east-1:jenkins-ec2-user") {
                            docker.image("ss-utopia-spark/spark-py:latest").push()
                        }
                    }
                    //sh 'aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com'
                    //sh 'docker push 422288715120.dkr.ecr.us-east-1.amazonaws.com/ss-utopia-spark/spark-py:latest'
                }
            }
        }
    }
}