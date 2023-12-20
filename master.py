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
            slave_information[node_id]["init_product"] = None
            leaf_nodes.append(node_id)
            if parent_id in leaf_nodes:
                leaf_nodes.remove(parent_id)
        leaf_nodes.sort()
        init_products = input_file.readlines()

        for i, node_id in enumerate(leaf_nodes):
            slave_information[node_id]["init_product"] = init_products[i].strip()
        
    node_data_template = {
        'num_production_cycles': num_production_cycles,
        'wear_factors': wear_factors,
        'treshold': treshold,
        'parent_id': None,
        'children': [],
        'init_state': None,
        'init_product': None,
    }
    
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    intercomm = MPI.COMM_SELF.Spawn(sys.executable,
                                    args=['worker.py'],
                                    maxprocs=num_machines+1)
    
    for i in range(num_machines):
        slave_data_json = slave_information[i+1]
        node_data_template['parent_id'] = slave_data_json['parent_id']
        node_data_template['children'] = slave_data_json['children']
        node_data_template['init_state'] = slave_data_json['init_state']
        node_data_template['init_product'] = slave_data_json['init_product']
        
        node_data_str = json.dumps(node_data_template)
        
        intercomm.send(node_data_str, dest=i+1)
    
    products = []
    maintenance = []
    product_messages = []
    maintenance_messages = []
    while len(product_messages) < num_production_cycles:
        if intercomm.Iprobe(source=MPI.ANY_SOURCE):
            status = MPI.Status()

            intercomm.Probe(source=MPI.ANY_SOURCE, status=status)
            node = status.source

            req = intercomm.irecv(source=node, tag=status.tag)
            data = req.wait()
            message = json.loads(data)
            if message['type']=='result':
                
                # print(message)
                if message not in product_messages:
                    product_messages.append(message)
                    products.append(message['message'])
                
            elif message['type']=='maintenance':
                if message not in maintenance_messages:
                    maintenance_messages.append(message)
                    maintenance.append(message['message'])
                

    # for i in product_messages:
    #     print(i['message'])
    
    def sorting_key(obj):
        return tuple(map(int, obj.split('-')))
    
    maintenance = sorted(maintenance, key=sorting_key)
    
    # for i in maintenance:
    #     print(i)
        
    exit(0)
            
    
        

if __name__ == '__main__':
    main()