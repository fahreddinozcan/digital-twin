from mpi4py import MPI
import json
import math

class ProductionNode():
    def __init__(self, node_id, parent_id, children, init_state, init_product, parent_comm, local_comm):
        self.node_id = node_id  # Machine is initialized with the data received
        self.parent_id = parent_id
        self.children = children
        self.state = init_state
        self.init_product = init_product
        self.accumulated_wear = 0  # Accumulated wear is initially 0
        self.current_production_cycle = 1  # First production cycle is 1
        self.parent_comm = parent_comm
        self.local_comm = local_comm

    
    def start_cycles(self):  # Machine starts its production cycles
        while(self.current_production_cycle <= NUM_PRODUCTION_CYCLES):  # Iterating through cycles
            preproducts = self.gather_products()  # Gathers the products of children
            product = self.produce(preproducts)  # Produces the product of the current cycle
            self.send_product(product)  # Sends the product either to another machine or to the control room
            self.change_state()  # Changes the state of the machine
            self.increment_production_cycle()  # Increments the production cycle by 1
        exit(0)


    def gather_products(self):  # Gathers the products of children
        products_of_children = []
        if self.children:  # If the machine is not a leaf machine
            for child_id in self.children:  # Receives the products of its children
                product_of_child_str = self.local_comm.recv(source=child_id)
                product_of_child_json = json.loads(product_of_child_str)
                products_of_children.append(product_of_child_json)

            sorted(products_of_children, key=lambda x: x.get('node_id', 0))  # Sorts the products of children according to their node ids
            return [child['product'] for child in products_of_children]
        else:  # If the machine is a leaf machine
            return [self.init_product]  # Returns the initial product
    

    def produce(self, preproducts):  # Produces the product of the current cycle
        preproducts = ''.join(preproducts)  # Adds the products of children
        if self.state == "add":  # Terminal node, no changes made to the product
            return preproducts
        elif self.state == "enhance":
            self.wear(WEAR_FACTORS[0])  # Wear operations
            return preproducts[0] + preproducts + preproducts[-1]  # First character + product + last character
        elif self.state == "reverse":
            self.wear(WEAR_FACTORS[1])  # Wear operations
            return preproducts[::-1]  # Reversed product
        elif self.state == "chop":
            self.wear(WEAR_FACTORS[2])  # Wear operations
            if len(preproducts) > 1:  # If the product is longer than 1 character
                return preproducts[:-1]  # Chop the last character
            else:
                return preproducts  # Do not change the product
        elif self.state == "trim":
            self.wear(WEAR_FACTORS[3])  # Wear operations
            if len(preproducts) > 2:  # If the product is longer than 2 characters
                return preproducts[1:-1]  # Trim the first and last characters
            else:
                return preproducts  # Do not change the product
        elif self.state == "split":
            self.wear(WEAR_FACTORS[4])  # Wear operations
            middle = math.ceil(len(preproducts) / 2)  # Point to split is calculated using ceiling function
            return preproducts[:middle]  # Split the product from approximately the middle
    

    def change_state(self):  # Changes the state of the machine
        if self.state == "trim":  # trim -> reverse -> trim -> reverse ...
            self.state = "reverse"
        elif self.state == "reverse":
            self.state = "trim"
        elif self.state == "split":  # split -> chop -> enhance -> split -> chop ...
            self.state = "chop"
        elif self.state == "chop":
            self.state = "enhance"
        elif self.state == "enhance":
            self.state = "split"

    
    def wear(self, wear_factor):  # Calculates the wear of the machine and sends a message to the control room if it needs maintenance
        self.accumulated_wear += wear_factor  # Wear is accumulated
        if self.accumulated_wear >= THRESHOLD:  # If the wear is reaches the threshold
            cost = self.calculate_cost(wear_factor)  # Cost of the maintenance is calculated
            message = self.prepare_message(cost)  # Maintenance log information is prepared
            self.message_master("maintenance", message)  # Sends a message to the control room in a non-blocking way
            self.reset_wear()  # Wear is reset to 0
    

    def calculate_cost(self, wear_factor):  # Calculates the cost of the maintenance
        return (self.accumulated_wear - THRESHOLD + 1) * wear_factor
    

    def prepare_message(self, cost):  # Prepares the maintenance log information to be sent to the control room
        return json.dumps({'node_id': self.node_id, 'cost': cost, 'cycle': self.current_production_cycle})
    

    def message_master(self, message_type, message_str):  # Sends a message to the control room in a non-blocking way
        data_str = json.dumps({'type': message_type, 'message_str': message_str})  # Message type can be "maintenance" or "result"
        self.parent_comm.isend(data_str, dest=0)


    def reset_wear(self):  # Resets the wear of the machine to 0
        self.accumulated_wear = 0

    
    def increment_production_cycle(self):  # Increments the production cycle by 1
        self.current_production_cycle += 1
    

    def send_product(self, product):  # Sends the product either to another machine or to the control room
        if self.parent_id:  # If the machine is not terminal
            self.local_comm.send(json.dumps({'product': product, 'node_id': self.node_id}), dest=self.parent_id)  # Send the product to the parent machine
        else:  # If the machine is terminal
            self.message_master("result", json.dumps({'product': product, 'cycle': self.current_production_cycle}))  # Send the product to the control room 
    

def main():
    parent_comm = MPI.Comm.Get_parent()  # Parent communicator, to communicate with the control room
    local_comm = MPI.COMM_WORLD  # Local communicator, to communicate within machines
    local_rank = local_comm.Get_rank()  # Rank of the worker in local communicator
    local_size = local_comm.Get_size()
    
    if local_rank == 0:  # We delete the 0th process, to match the ranks with the node ids
        exit(0)
        
    json_str = parent_comm.recv(source=0)  # Receive the machine initialization data from the control room
    node_data = json.loads(json_str)  # Parse the json string to a dictionary

    # Global variables are initialized from the data received
    global NUM_PRODUCTION_CYCLES
    NUM_PRODUCTION_CYCLES = node_data['num_production_cycles']

    global WEAR_FACTORS
    WEAR_FACTORS = node_data['wear_factors']

    global THRESHOLD
    THRESHOLD = node_data['treshold']
    
    production_node = ProductionNode(local_rank,  # Machine is initialized with the data received
                                     node_data['parent_id'],
                                     node_data['children'],
                                     node_data['init_state'],
                                     node_data['init_product'],
                                     parent_comm,
                                     local_comm
                                     )

    production_node.start_cycles()  # Machine starts its production cycles
        
        

if __name__ == '__main__':
    main()