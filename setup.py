import boto3
import configparser
import json
import time

def create_redshift_role(iam, ROLE_NAME):
    """
    Description: This function can be used to create IAM role that 
    allows redshift readonly access to S3 bucket.

    Arguments:
        iam: iam client object. 
        ROLE_NAME: the name for the new role to be created. 

    Returns:
        None. 
    """
    try:
        #Creating the role 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

        
def attach_s3_read_only_access(iam, ROLE_NAME):
    """
    Description: This function can be used to attach S3 readonly access 
    policy to role created for redshift.

    Arguments:
        iam: iam client object. 
        ROLE_NAME: Name of the role to which policy is to be attached. 

    Returns:
        None. 
    """
    try:
        #Attaching role policy
        iam.attach_role_policy(RoleName=ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )
    except Exception as e:
        print(e)
    

def get_arn(iam, ROLE_NAME):
    """
    Description: This function can be used to retrieve role arn of the
    role created for redshift.

    Arguments:
        iam: iam client object. 
        ROLE_NAME: Name of the role to retrieve its role arn. 

    Returns:
        roleArn: Arn(Amazon resource name) of the role. 
    """
    try:
        # Retrieve IAM role ARN
        roleArn = iam.get_role(RoleName=ROLE_NAME)['Role']['Arn']
        return roleArn
    except Exception as e:
        print(e)
    
    
def create_redshift_cluster(redshift, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, DB_NAME, CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD, ARN):
    """
    Description: This function can be used to create a redshift cluster.

    Arguments:
        redshift: redshift client object. 
        CLUSTER_TYPE: Cluster type. Example- 'multi-node' cluster. 
        NODE_TYPE: Node type. dc2.large for a compute node.
        NUM_NODES: Number of nodes.        
        DB_NAME: Database name.
        CLUSTER_IDENTIFIER: Cluster name.
        DB_USER: Database username.
        DB_PASSWORD: Database password.
        ARN: Amazon resource name, that is, Role ARN.
    
    Returns:
        None. 
    """
    try:
        # Creating a Redshift cluster
        response = redshift.create_cluster(        
            #HW
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
        
            #Roles (for s3 access)
            IamRoles=[ARN]
        )
        print('Creating cluster...')
    except Exception as e:
        print(e)

        
def get_cluster_endpoint(redshift, CLUSTER_IDENTIFIER):
    """
    Description: This function can be used to check redshift cluster 
    availability and to return cluster endpoint.

    Arguments:
        redshift: redshift client object. 
        CLUSTER_IDENTIFIER: Cluster name.
    
    Returns:
        Cluster endpoint address.
    """
    try:
        clusterStatus = ''
        myClusterProps = {}
        # Verify cluster availability
        while(clusterStatus != 'available'):            
            time.sleep(400)
            print('Checking cluster status')
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
            clusterStatus = myClusterProps['ClusterStatus']   
        print('Cluster created successfully.')
        
        # return cluster endpoint
        return myClusterProps['Endpoint']['Address']
        
    except Exception as e:
        print(e)    

def cluster_open_incoming_tcp_port(redshift, ec2, CLUSTER_IDENTIFIER, DB_PORT):
    """
    Description: This function can be used to allow incoming traffic to
    redshift cluster.

    Arguments:
        redshift: redshift client object. 
        ec2: ec2 resource.
        CLUSTER_IDENTIFIER: Cluster name.
        DB_PORT: Database port number.
    
    Returns:
        None.
    """
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        
        # Authorize ingress for the default security group for incoming traffic
        # from any IP address using TCP protocol.
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)
           
    
def main():
    """
    Description: This function can be used for setup actions such as creating
    IAM role for redshift cluster, attaching policy to the role, creating
    redshift cluster, and allowing ingress of incoming traffic to cluster's 
    default security group.

    Arguments:
        None.
    
    Returns:
        None.
    """
    
    # Loading DWH params from file
    print('Loading DWH params from file')
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                = config.get("CLUSTER","KEY")
    SECRET             = config.get("CLUSTER","SECRET")
    CLUSTER_TYPE       = config.get("CLUSTER","CLUSTER_TYPE")
    NUM_NODES          = config.get("CLUSTER","NUM_NODES")
    NODE_TYPE          = config.get("CLUSTER","NODE_TYPE")
    CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
    DB_NAME            = config.get("CLUSTER","DB_NAME")
    DB_USER            = config.get("CLUSTER","DB_USER")
    DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    HOST               = config.get("CLUSTER","HOST")
    DB_PORT            = config.get("CLUSTER","DB_PORT")
    ROLE_NAME          = config.get("CLUSTER","ROLE_NAME")  
    print('Loaded DWH params from file')

    # Creating client or resource for iam, ec2 and redshift services
    print('Creating client or resource for iam, ec2 and redshift services')
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )
    
    redshift = boto3.client('redshift',
                               region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                           )
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    print('Created client or resource for iam, ec2 and redshift services')
    
    # Creating IAM role for redshift 
    print('Creating IAM role for redshift')
    create_redshift_role(iam, ROLE_NAME)
    print('Created IAM role for redshift')
    
    # Attaching S3 Read only access to specified role
    print('Attaching S3 Read only access to specified role')
    attach_s3_read_only_access(iam, ROLE_NAME)
    print('Attached S3 Read only access to specified role')
    
    # Retrieving ARN for the given role
    print('Retrieving ARN for the given role')
    ARN = get_arn(iam, ROLE_NAME)
    print('Retrieved ARN "{}" for the given role'.format(ARN))
    
    # Updating config file with ARN
    print('Updating config file with ARN')
    config.set('IAM_ROLE','ARN',ARN)
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
    print('Updated config file with ARN')
        
    # Creating Redshift cluster
    print('Creating Redshift cluster')
    create_redshift_cluster(redshift, CLUSTER_TYPE, NODE_TYPE, NUM_NODES, DB_NAME, CLUSTER_IDENTIFIER, DB_USER, DB_PASSWORD, ARN)
    print('Created Redshift cluster')
    
    # Retrieving cluster endpoint 
    print('Retrieving cluster endpoint')
    HOST = get_cluster_endpoint(redshift, CLUSTER_IDENTIFIER)
    print('Retrieved cluster endpoint "{}"'.format(HOST))
    
    # Updating config file with host
    print('Updating config file with host')
    config.set('CLUSTER','HOST',HOST)
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
    print('Updated config file with host')

   # Opening incoming TCP port to access cluster endpoint
    print('Opening incoming TCP port to access cluster endpoint')
    cluster_open_incoming_tcp_port(redshift, ec2, CLUSTER_IDENTIFIER, DB_PORT)
    print('Opened incoming TCP port to access cluster endpoint')
    
        
if __name__ == "__main__":
    main()

