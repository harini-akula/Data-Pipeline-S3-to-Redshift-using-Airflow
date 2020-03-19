import boto3
import configparser
import time

def revoke_security_group_ingress(redshift, ec2, CLUSTER_IDENTIFIER, DB_PORT):
    """
    Description: This function can be used to revoke ingress of incoming 
    traffic to redshift cluster.

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
        
        # Revoking ingress for the default security group.
        defaultSg.revoke_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)

def delete_redshift_cluster(redshift, CLUSTER_IDENTIFIER): 
    """
    Description: This function can be used to delete the redshift cluster.

    Arguments:
        redshift: redshift client object. 
        CLUSTER_IDENTIFIER: Cluster name.
    
    Returns:
        None.
    """
    try:
        redshift.delete_cluster( ClusterIdentifier=CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        print('Deleting redshift cluster...')
    except Exception as e:
        print(e)

    
def cluster_status_change(redshift, CLUSTER_IDENTIFIER):   
    """
    Description: This function can be used to verify deletion of
    redshift cluster.

    Arguments:
        redshift: redshift client object. 
        CLUSTER_IDENTIFIER: Cluster name.
    
    Returns:
        None.
    """
    try:
        clusterStatus = ''
        myClusterProps = {}
        # Checking cluster status
        while(clusterStatus != 'deleting'):
            time.sleep(500)
            print('Checking cluster status')
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
            clusterStatus = myClusterProps['ClusterStatus']
        print('Reshift cluster deleted successfully')
        
    except Exception as e:
        print(e) 
        

def detach_policy_delete_role(iam, ROLE_NAME):
    """
    Description: This function can be used to detach S3 readonly access policy
    from the IAM role and delete the IAM role.

    Arguments:
        iam: iam client object. 
        ROLE_NAME: Name of the role from which policy is to be detached.
        and role to be deleted. 

    Returns:
        None. 
    """
    try:
        # Detaching role policy.
        iam.detach_role_policy(RoleName=ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    
        # Deleting IAM role.
        iam.delete_role(RoleName=ROLE_NAME)

    except Exception as e:
        print(e) 
 

def main():
    """
    Description: This function can be used for teardown actions such as deleting
    IAM role for redshift cluster, detaching policy to the role, deleting
    redshift cluster, and revoking ingress of incoming traffic to cluster's 
    default security group.

    Arguments:
        None.
    
    Returns:
        None.
    """    
    # Load params from the file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    ROLE_NAME = config.get('CLUSTER', 'ROLE_NAME')
    DB_PORT = config.get("CLUSTER","DB_PORT")
    KEY = config.get("CLUSTER","KEY")
    SECRET = config.get("CLUSTER","SECRET")
    
    # Creating client or resource for iam and redshift services
    ec2 = boto3.resource('ec2',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                    )
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )
    redshift = boto3.client('redshift',
                               region_name="us-west-2",
                               aws_access_key_id=KEY,
                               aws_secret_access_key=SECRET
                           )    
   
    # Revoking ingress for security group
    revoke_security_group_ingress(redshift, ec2, CLUSTER_IDENTIFIER, DB_PORT)

    # Deleting redshift cluster
    delete_redshift_cluster(redshift, CLUSTER_IDENTIFIER)

    # Wait for cluster status to be updated to deleting
    cluster_status_change(redshift, CLUSTER_IDENTIFIER)

    # Detaching role policy and deleting role
    detach_policy_delete_role(iam, ROLE_NAME)


if __name__== "__main__":
    main()
