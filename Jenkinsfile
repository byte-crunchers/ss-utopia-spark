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
                    sh 'mv -f mysql.jar jars/mysql.jar'
                    sh 'curl -o spark-streaming-kinesis-asl-assembly_2.12-3.3.0.jar http://ss-utopia-build-resources.s3.amazonaws.com/jars/spark-streaming-kinesis-asl-assembly_2.12-3.3.0.jar'
                    sh 'mv spark-streaming-kinesis-asl-assembly_2.12-3.3.0.jar jars/spark-streaming-kinesis-asl-assembly_2.12-3.3.0.jar'
                }
            }
        }
        stage('Get Secrets'){
            steps {
                script {
                    env.MYSQL_USER = sh ( script: 'aws secretsmanager get-secret-value --secret-id spark  | jq --raw-output .SecretString | jq -r ."user"', returnStdout: true)
                    env.MYSQL_PASS = sh(script: 'aws secretsmanager get-secret-value --secret-id spark  | jq --raw-output .SecretString | jq -r ."pass"',returnStdout: true)
                    env.MYSQL_LOC = sh(script: 'aws secretsmanager get-secret-value --secret-id spark  | jq --raw-output .SecretString | jq -r ."location"',returnStdout: true)
                    //If I don't do ACC_ID this way, Jenkins puts a space in front of it. I know not why
                    sh(script:'ACC_ID=$(aws secretsmanager get-secret-value --secret-id spark  | jq --raw-output .SecretString | jq -r ."account_id")')
                }
            }
        }
        stage('Pull') {
            steps {
                dir('spark-stuff'){
                    sh 'ls'
                    dir('ss-utopia-spark'){
                        git branch: 'develop', url: 'https://github.com/byte-crunchers/ss-utopia-spark' //perameterize with env
                    }
                    sh 'mv -f ss-utopia-spark/Dockerfile kubernetes/dockerfiles/spark/bindings/python/Dockerfile'
                    sh 'mv -f ss-utopia-spark/log4j.properties conf/log4j.properties'
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
                    }//prune
                    //sh 'aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${ACC_ID}.dkr.ecr.us-east-1.amazonaws.com'
                    //sh 'docker push ${ACC_ID}.dkr.ecr.us-east-1.amazonaws.com/ss-utopia-spark/spark-py:latest'
                }
            }
        }
        stage('Create Cluster'){
            steps{
                //This command will skip if cluster 'Spark' already exists
                sh 'export PATH=$PATH:/usr/local/bin/' //I need to do this so that eksctl is recognized
                sh 'eksctl create cluster --region us-east-1 --zones us-east-1a,us-east-1b,us-east-1c --name Spark --fargate || true 2> /dev/null'
                sh 'aws eks --region us-east-1 update-kubeconfig --name Spark'
                sh 'kubectl create serviceaccount spark || true 2> /dev/null' //needed so the driver can create more pods
                sh 'kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default || true 2> /dev/null'
                //give access to henry
                sh 'eksctl create iamidentitymapping --cluster  Spark --arn arn:aws:iam::${ACC_ID}:user/henry.admin --group system:masters --username admin1'
                sh 'eksctl create iamidentitymapping --cluster  Spark --arn arn:aws:iam::${ACC_ID}:user/wyatt.admin --group system:masters --username admin2'
                sh 'kubectl delete pods --all' //if spark is already running, kill it
                script{
                    //save cluster endpoint
                    env.CLUSTER = sh ( script: 'aws eks describe-cluster --name Spark | jq --raw-output .cluster |jq --raw-output .endpoint', returnStdout: true)
                }


            }
        }
        stage('Deploy'){
            steps{
                dir('spark-stuff'){
                    //For this we're using Apache's native spark-submit tool. It has a lot of options.
                    sh './bin/spark-submit --master k8s://${CLUSTER} \
                    --deploy-mode cluster \
                    --name byte-consumer \
                    --conf spark.executor.instances=2  \
                    --conf spark.kubernetes.executor.podNamePrefix=executor \
                    --conf spark.kubernetes.executor.request.cores=2 \
                    --conf spark.executor.cores=2 \
                    --conf spark.executor.memory=2g \
                    --conf spark.kubernetes.submission.waitAppCompletion=false \
                    --conf spark.kubernetes.driver.pod.name=driver \
                    --conf spark.kubernetes.driverEnv.ACCESS_KEY=${AWS_ACCESS_KEY_ID} \
                    --conf spark.kubernetes.driverEnv.SECRET_KEY=${AWS_SECRET_ACCESS_KEY} \
                    --conf spark.kubernetes.driverEnv.AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
                    --conf spark.kubernetes.driverEnv.MYSQL_USER=${MYSQL_USER} \
                    --conf spark.kubernetes.driverEnv.MYSQL_PASS=${MYSQL_PASS}  \
                    --conf spark.kubernetes.driverEnv.MYSQL_LOC=${MYSQL_LOC}  \
                    --conf spark.kubernetes.driverEnv.CONSUMER_NAME=cloud-consumer \
                    --conf spark.kubernetes.driverEnv.MAX_THREADS=20 \
                    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
                    --conf spark.kubernetes.container.image.pullPolicy=Always \
                    --conf spark.kubernetes.container.image=${ACC_ID}.dkr.ecr.us-east-1.amazonaws.com/ss-utopia-spark/spark-py:latest \
                    local:///opt/spark/work-dir/stream_consumer.py'
                }
            }
        }
    }
}
