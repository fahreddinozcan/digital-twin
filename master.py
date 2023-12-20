import sys
import json
from mpi4py import MPI


def main():
    input_file_path = sys.argv[1]  # I/O file paths are fetched from the command line arguments
    output_file_path = sys.argv[2]
    with open(input_file_path, 'r') as input_file:  # Reading the input file
        num_machines = int(input_file.readline())  # General information is read from the input file
        num_production_cycles = int(input_file.readline())
        wear_factors = list(map(int, input_file.readline().split()))
        treshold = int(input_file.readline())
        machine_information = {1: {"parent_id": None, "children": [], "init_state": "add", "init_product": None}}  # Machine information dictionary is initialized with the terminal machine
        leaf_machines = []
        for i in range(num_machines - 1):  # Information regarding the machines are read from the input file
            input_arr = input_file.readline().split()
            node_id = int(input_arr[0])
            parent_id = int(input_arr[1])
            init_state = input_arr[2]
            machine_information[node_id] = {"parent_id": parent_id, "children": [], "init_state": init_state}  # Machine information is stored
            machine_information[parent_id]["children"].append(node_id)  # Parent machine's children list is updated
            machine_information[node_id]["init_product"] = None
            leaf_machines.append(node_id)  # New machine is added to the leaf machines list
            if parent_id in leaf_machines:
                leaf_machines.remove(parent_id)  # If the parent was still in the leaf machines, it is removed
        leaf_machines.sort()  # Leaf machine ids are sorted
        init_products = input_file.readlines()  # Initial products are read from the input file

        for i, node_id in enumerate(leaf_machines):  # Initial products are assigned to the leaf machines
            machine_information[node_id]["init_product"] = init_products[i].strip()
        
    node_data_template = {  # Template for the machine initialization information to be sent to the machines
        'num_production_cycles': num_production_cycles,
        'wear_factors': wear_factors,
        'treshold': treshold,
        'parent_id': None,
        'children': [],
        'init_state': None,
        'init_product': None,
    }
    
    comm = MPI.COMM_WORLD  # MPI communicator is initialized
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    intercomm = MPI.COMM_SELF.Spawn(sys.executable,
                                    args=['worker.py'],
                                    maxprocs=num_machines+1)
    
    for i in range(num_machines):
        slave_data_json = machine_information[i+1]
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
            if data['type']=='result':  # If the message is a product message
                message = json.loads(data['message_str'])  # Product information is parsed and stored
                product, cycle = message['product'], message['cycle']
                products.append((product, cycle))
                if len(products) == num_production_cycles:  # If all products are received, the loop is broken
                    break
                
            elif data['type']=='maintenance':  # If the message is a maintenance log message
                message = json.loads(data['message_str'])  # Maintenance log information is parsed and stored
                node_id, cost, cycle = message['node_id'], message['cost'], message['cycle']
                maintenance_logs.append((node_id, cost, cycle))
    
    products.sort(key=lambda x: x[1])  # Sorting the products according to their production cycles
    maintenance_logs.sort(key=lambda x: (x[0], x[2], x[1]))  # Sorting the maintenance logs according to their node ids, production cycles and costs respectively

    with open(output_file_path, 'w') as output_file:  # Writing the output file
        for product, cycle in products:  # Writing the products
            output_file.write(f"{product}\n")
        for i, (node_id, cost, cycle) in enumerate(maintenance_logs):  # Writing the maintenance logs
            if i != len(maintenance_logs) - 1:
                output_file.write(f"{node_id}-{cost}-{cycle}\n")
            else:
                output_file.write(f"{node_id}-{cost}-{cycle}")
        
    exit(0)
            


if __name__ == '__main__':
    main()