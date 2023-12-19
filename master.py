import sys
import json
from mpi4py import MPI


def main():
    input_file_name = sys.argv[1]
    output_file_name = sys.argv[2]
    with open(input_file_name, 'r') as input_file:
        num_machines = int(input_file.readline())
        num_production_cycles = int(input_file.readline())
        wear_factors = list(map(int, input_file.readline().split()))
        treshold = int(input_file.readline())
        slave_information = {1: {"parent_id": None, "children": [], "init_state": "add", "init_product": None}}
        leaf_nodes = []
        for i in range(num_machines - 1):
            input_arr = input_file.readline().split()
            node_id = int(input_arr[0])
            parent_id = int(input_arr[1])
            init_state = input_arr[2]
            slave_information[node_id] = {"parent_id": parent_id, "children": [], "init_state": init_state}
            slave_information[parent_id]["children"].append(node_id)
            leaf_nodes.append(node_id)
            if parent_id in leaf_nodes:
                leaf_nodes.remove(parent_id)
        leaf_nodes.sort()
        init_products = input_file.readlines()
        for leaf_node_id, init_product in zip(leaf_nodes, init_products):
            slave_information[leaf_node_id]["init_product"] = init_product
            
        
        # num_production_cycles wear_factors treshold slave_information[relevant]
        arguments = []
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    if rank == 0:
            intercomm = MPI.COMM_SELF.Spawn(sys.executable,
                                         args=['worker.py'],
                                         maxprocs=11)
            
            for i in range(10):
                slave_data_json = slave_information[i+1]
                slave_data_str = json.dumps(slave_data_json)
                
                intercomm.send(slave_data_str, dest=i+1)
        

if __name__ == '__main__':
    main()