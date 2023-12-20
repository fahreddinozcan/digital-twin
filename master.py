import sys
import json
from mpi4py import MPI


def main():
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    with open(input_file_path, 'r') as input_file:
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
    maintenance_logs = []
    while True:
        if intercomm.Iprobe(source=MPI.ANY_SOURCE):
            status = MPI.Status()

            intercomm.Probe(source=MPI.ANY_SOURCE, status=status)
            node = status.source

            req = intercomm.irecv(source=node, tag=status.tag)
            data_str = req.wait()
            data = json.loads(data_str)
            if data['type']=='result':
                message = json.loads(data['message_str'])
                product, cycle = message['product'], message['cycle']
                products.append((product, cycle))
                if len(products) == num_production_cycles:
                    break
                
            elif data['type']=='maintenance':
                message = json.loads(data['message_str'])
                node_id, cost, cycle = message['node_id'], message['cost'], message['cycle']
                maintenance_logs.append((node_id, cost, cycle))
    
    products.sort(key=lambda x: x[1])
    maintenance_logs.sort(key=lambda x: (x[0], x[2], x[1]))

    with open(output_file_path, 'w') as output_file:
        for product, cycle in products:
            output_file.write(f"{product}\n")
        for i, (node_id, cost, cycle) in enumerate(maintenance_logs):
            if i != len(maintenance_logs) - 1:
                output_file.write(f"{node_id}-{cost}-{cycle}\n")
            else:
                output_file.write(f"{node_id}-{cost}-{cycle}")
        
    exit(0)
            
    
        

if __name__ == '__main__':
    main()