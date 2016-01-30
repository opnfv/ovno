#!/bin/bash
# FUNCTEST_REPO_DIR=/tmp
# Setup environment
export IMAGE_DIR="$FUNCTEST_REPO_DIR/ovno/images"
export TESTCASE_DIR="$FUNCTEST_REPO_DIR/ovno/Testcases"
export CIRROS_IMAGE=Cirros.img
export FW_IMAGE="$FUNCTEST_REPO_DIR/ovno/images/OpenWRT-LRM--fw-v3.img"
export RESULTS_DIR="$FUNCTEST_REPO_DIR/ovno/Testresults/results.`date +%Y-%m-%d:%H:%M:%S`"
export RESULTS_FILE="$RESULTS_DIR/ocl_results"
export ERR_FILE="$RESULTS_DIR/ocl__err"
export TEST_ERR=/tmp/test_err
mkdir -p $RESULTS_DIR


check_test(){
echo $test_desc
if [ $($test > $TEST_ERR) ]
then
  echo "Success     $test_name" >> $RESULTS_FILE
else
  echo "FAIL        $test_name" >> $ERR_FILE
  cat $TEST_ERR >> $ERR_FILE
fi
}
#------------------------------------------------------------------------------#
# Go to the where the scripts are 

cd $TESTCASE_DIR

export API_IP=`echo $OS_AUTH_URL | cut -d'/' -f3`

export PATH=.:$PATH

mkdir $LOG_DIR

#___________________________________________________________________
#Load images into OpenStack


# Get the Cirros image onto jump/test server, then load into OpenStack
glance image-create --name Cirros --file $IMAGE_DIR/$CIRROS_IMAGE --disk-format qcow2 --container-format bare --owner $TENANT_ID

# Get firewall image 
glance image-create --name fw --file  $IMAGE_DIR/$FW_IMAGE --disk-format qcow2 --container-format bare --owner $TENANT_ID


export NET1="192.168.1.0/24"
export NET2="192.168.2.0/24"
export NET3="192.168.3.0/24"
export NET_PUBLIC="10.8.10.0/24"


# Set up the test list
cat<<EOF | sed  -e "/#/d" >  Test_V4 
Add IP address manager|add_ipam|config add ipam ipam 
Add network Net1|add_network_Net1|config add network N1 --ipam ipam --subnet $NET1
Add VM V11|add_VM_V11|config add vm V11 --image Cirros --flavor m1.tiny --network Net1
Add VM V12|add_VM_V12|config add vm V12 --image Cirros --flavor m1.tiny --network Net1
#Check V11 to V12 connectivity!! Still to be implemented !!
Add policy Net2-Net3|add_plcy_Net2-Net3|config add policy Net2-Net3 --rule src-net=Net1,dst-net=Net2config add network Net2 --ipam ipam --subnet 192.168.2.0/24 --policy Net2-Net3
Add network Net2|add_net_Net2|config add network Net2 --ipam ipam --subnet 192.168.2.0/24 --policy Net2-Net3
Add network Net3|add_net_Net3|config add network Net3 --ipam ipam --subnet 192.168.2.0/24 --policy Net2-Net3
Add VM V21|add_VM_VM21|config add vm V21 --image Cirros --flavor m1.tiny --network Net2
Add VM V31|add_VM_VM31|config add vm V31 --image Cirros --flavor m1.tiny --network Net3
#Check V21 to V31 connectivity !! Still to be implemented !!
Add svc tmplt fw-l3|add_ST_fw-l3|config add service-template fw-l3 --mode in-network --type firewall --image fw --flavor m1.medium --interface left --interface right --interface management
Add service inst fw|add_svc_fw|config add service-instance fw --template fw-l3 --network network=left,tenant=$TENANT --network network=right,tenant=$TENANT --network=auto,tenant=$TENANT
Add svc chain policy|add_svc_chain_plcy|config add policy left-right --rule src-net=left,dst-net=right --action service --service fw
Add network left|add_network_left|config add network left --ipam ipam --subnet $NET2 --policy left-right
Add network right|add_network_right|config add network right --ipam ipam --subnet $NET3 --policy left-right
Add VM VL1|add_VM_VL1|config add vm VL1 --image Cirros --flavor m1.tiny --network left
Add VM VR1|add_VM_VR1|config add vm VR1 --image Cirros --flavor m1.tiny --network right
#Check V21 to V31 connectivity !! Still to be implemented !!
Add network public|add_net_public|config add network public --ipam ipam-default --sbunet $NET_PUBLIC --route-target 64512:10000
Add floating IP pool|add_float_ip_pool|config add floating-ip-pool public-pool --network public
Add floating IP to VM|add_float_ip_vm|config add vm-interface server|V11 --floating-ip --floating-ip-pool public-pool
# Check external connectivity to V11 !! Still to be implemented !!
Clean up v4|clean_up_v4|cleanup v4
EOF

while IFS='|' read test_desc test_name test
do
  check_test
done <Test_V4

# IPv6 tests
export NET1="xx"
export NET2="yy"
export NET3="zz"
export NET_PUBLIC="aa"

cat<<EOF | sed  -e "/#/d" >  Test_V6
V6 IP V6ress manager|V6_ipam|config add ipam ipam 
V6 network Net1|V6_network_Net1|config add network N1 --ipam ipam --subnet $NET1
V6 VM V11|V6_VM_V11|config add vm V11 --image Cirros --flavor m1.tiny --network Net1
V6 VM V12|V6_VM_V12|config add vm V12 --image Cirros --flavor m1.tiny --network Net1
# Check V11 to V12 connectivity!! Still to be implemented !!
V6 policy Net2-Net3|V6_plcy_Net2-Net3|	config add policy Net2-Net3 --rule src-net=Net1,dst-net=Net2config V6 network Net2 --ipam ipam --subnet 192.168.2.0/24 --policy Net2-Net3
V6 network Net2|V6_net_Net2|config add network Net2 --ipam ipam --subnet 192.168.2.0/24 --policy Net2-Net3
V6 network Net3|V6_net_Net3|config add network Net3 --ipam ipam --subnet 192.168.2.0/24 --policy Net2-Net3
V6 VM V21|V6_VM_VM21|config add vm V21 --image Cirros --flavor m1.tiny --network Net2
V6 VM V31|V6_VM_VM31|config add vm V31 --image Cirros --flavor m1.tiny --network Net3
# Check V21 to V31 connectivity !! Still to be implemented !!
V6 svc tmplt fw-l3|V6_ST_fw-l3|config add service-template fw-l3 --mode in-network --type firewall --image fw --flavor m1.medium --interface left --interface right --interface management
V6 service inst fw|V6_svc_fw|config add service-instance fw --template fw-l3 --network network=left,tenant=$TENANT --network network=right,tenant=$TENANT --network=auto,tenant=$TENANT
V6 svc chain policy|V6_svc_chain_plcy|config add policy left-right --rule src-net=left,dst-net=right --action service --service fw
V6 network left|V6_network_left|config add network left --ipam ipam --subnet $NET2 --policy left-right
V6 network right|V6_network_right|config add network right --ipam ipam --subnet $NET3 --policy left-right
V6 VM VL1|V6_VM_VL1|config add vm VL1 --image Cirros --flavor m1.tiny --network left
V6 VM VR1|V6_VM_VR1|config add vm VR1 --image Cirros --flavor m1.tiny --network right
# Check V21 to V31 connectivity !! Still to be implemented !!
V6 network public|V6_net_public|config add network public --ipam ipam-default --sbunet $NET_PUBLIC --route-target 64512:10000
V6 floating IP pool|V6_float_ip_pool|config add floating-ip-pool public-pool --network public
V6 floating IP to VM|V6_float_ip_vm|config add vm-interface server|V11 --floating-ip --floating-ip-pool public-pool
# Check external connectivity to V11 !! Still to be implemented !!
Clean up v6|clean_up_v6|cleanup v6
EOF

while IFS='|' read test_desc test_name test
do
  check_test
done <Test_V6


# After this send results to database
