pipeline {
    agent any

    stages {
        stage('Setup'){
            
            environment {
                SPARK_VERSION = '3.2.0'
                ASSEMBLY_SPARK_VERSION = '3.3.0'
                HADOOP_VERSION = '3.2'
                MYSQL_JAR_VERSION = '8.0.27'
                SCALA_VERSION = '2.12'
            }
            steps{
                sh 'rm -rf spark-stuff 2> /dev/null'
                sh "curl -o spark.tgz https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
                sh 'tar zxf spark.tgz'
                sh "mv -f 'spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}' spark-stuff"
                dir ('spark-stuff'){
                    sh "curl -o mysql.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/${MYSQL_JAR_VERSION}/mysql-connector-java-${MYSQL_JAR_VERSION}.jar"
                    sh 'mv -f mysql.jar jars/mysql.jar'
                    sh "curl -o spark-streaming-kinesis-asl-assembly_${SCALA_VERSION}-${ASSEMBLY_SPARK_VERSION}.jar http://ss-utopia-build-resources.s3.amazonaws.com/jars/spark-streaming-kinesis-asl-assembly_${SCALA_VERSION}-${ASSEMBLY_SPARK_VERSION}.jar"
                    sh "mv spark-streaming-kinesis-asl-assembly_${SCALA_VERSION}-${ASSEMBLY_SPARK_VERSION}.jar jars/spark-streaming-kinesis-asl-assembly_${SCALA_VERSION}-${ASSEMBLY_SPARK_VERSION}.jar"
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
                    dir('ss-utopia-spark'){
                        git branch: "${BRANCH}", url: 'https://github.com/byte-crunchers/ss-utopia-spark' //perameterize with env
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
                        docker.withRegistry("https://${ACC_ID}.dkr.ecr.us-east-1.amazonaws.com", "ecr:us-east-1:jenkins-ec2-user") {
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
                sh "eksctl create iamidentitymapping --cluster  Spark --arn arn:aws:iam::${ACC_ID}:user/henry.admin --group system:masters --username admin1"
                sh "eksctl create iamidentitymapping --cluster  Spark --arn arn:aws:iam::${ACC_ID}:user/wyatt.admin --group system:masters --username admin2"
                sh 'kubectl delete pods --all' //if spark is already running, kill it
                script{
                    //save cluster endpoint
                    env.CLUSTER = sh ( script: 'aws eks describe-cluster --name Spark | jq --raw-output .cluster |jq --raw-output .endpoint', returnStdout: true)
                }


            }
        }
        stage('Deploy'){
             environment {
                NUM_EXECUTORS = '2'
                MAX_EXECUTORS = '6'
                NUM_CORES = '2'
                THREADS = '6' //how many threads and therefore db connections per task
                EXECUTOR_MEMORY = "1500m" 
                DRIVER_MEMORY = "2g" 
                SUSTAINED_TIMEOUT = "4m" //how long after requesting new executors does it ask for more if need be
                PARTITIONS = '10' 
                BATCH_LENGTH = '10' //10 second batches because the processing of each batch takes a minimum of a few seconds.
                SCALING_INTERVAL = '60' //every interval the autoscaler checks to see if more or fewer pods are needed


             }
            steps{
                dir('spark-stuff'){
                    //For this we're using Apache's native spark-submit tool. It has a lot of options.
                    sh './bin/spark-submit --master k8s://${CLUSTER} \
                    --deploy-mode cluster \
                    --name byte-consumer \
                    --conf spark.streaming.dynamicAllocation.shuffleTracking.enabled=true \
                    --conf spark.streaming.dynamicAllocation.enabled=true \
                    --conf spark.streaming.dynamicAllocation.minExecutors=2 \
                    --conf spark.streaming.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
                    --conf spark.streaming.dynamicAllocation.scalingInterval=${SCALING_INTERVAL} \
                    --conf spark.kubernetes.executor.podNamePrefix=executor \
                    --conf spark.kubernetes.executor.request.cores=${NUM_CORES} \
                    --conf spark.executor.cores=${NUM_CORES} \
                    --conf spark.executor.memory=${EXECUTOR_MEMORY} \
                    --conf spark.driver.memory=${DRIVER_MEMORY} \
                    --conf spark.kubernetes.submission.waitAppCompletion=false \
                    --conf spark.kubernetes.driver.pod.name=driver \
                    --conf spark.kubernetes.driverEnv.ACCESS_KEY=${AWS_ACCESS_KEY_ID} \
                    --conf spark.kubernetes.driverEnv.SECRET_KEY=${AWS_SECRET_ACCESS_KEY} \
                    --conf spark.kubernetes.driverEnv.AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} \
                    --conf spark.kubernetes.driverEnv.CONSUMER_NAME=cloud-consumer \
                    --conf spark.kubernetes.driverEnv.BATCH_LENGTH=${BATCH_LENGTH} \
                    --conf spark.executorEnv.MYSQL_USER=${MYSQL_USER} \
                    --conf spark.executorEnv.MYSQL_PASS=${MYSQL_PASS} \
                    --conf spark.executorEnv.MYSQL_LOC=${MYSQL_LOC}  \
                    --conf spark.executorEnv.ACCESS_KEY=${AWS_ACCESS_KEY_ID} \
                    --conf spark.executorEnv.SECRET_KEY=${AWS_SECRET_ACCESS_KEY} \
                    --conf spark.executorEnv.MAX_THREADS=${THREADS} \
                    --conf spark.kubernetes.driverEnv.PARTITIONS=${PARTITIONS} \
                    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
                    --conf spark.kubernetes.container.image.pullPolicy=Always \
                    --conf spark.kubernetes.container.image=${ACC_ID}.dkr.ecr.us-east-1.amazonaws.com/ss-utopia-spark/spark-py:latest \
                    local:///opt/spark/work-dir/stream_consumer.py'
                }
            }
        }
    }
}
