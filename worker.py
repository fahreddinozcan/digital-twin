from mpi4py import MPI
import json
import math

class ProductionNode():
    def __init__(self, node_id, parent_id, children, init_state, init_product, parent_comm, local_comm):
        self.node_id = node_id
        self.parent_id = parent_id
        self.children = children
        self.state = init_state
        self.init_product = init_product
        self.accumulated_wear = 0
        self.current_production_cycle = 1
        self.parent_comm = parent_comm
        self.local_comm = local_comm

    
    def start_cycles(self):
        while(self.current_production_cycle <= NUM_PRODUCTION_CYCLES):
            preproducts = self.gather_products()
            product = self.produce(preproducts)
            if self.node_id == 1:
                # print(f"SENDING: {product}")
                self.message_master("result", product)
            self.send_product(product)
            self.change_state()
            self.increment_production_cycle()
        exit(0)


    def gather_products(self):
        products_of_children = []
        if self.children:
            for child_id in self.children:
                product_of_child_str = self.local_comm.recv(source=child_id)
                product_of_child_json = json.loads(product_of_child_str)
                products_of_children.append(product_of_child_json)

            sorted(products_of_children, key=lambda k: k.get('node_id', 0))
            return [child['product'] for child in products_of_children]
        else:
            return [self.init_product]
    

    def produce(self, preproducts):
        preproducts = ''.join(preproducts)
        if self.state == "add":
            return preproducts
        if self.state == "enhance":
            self.wear(WEAR_FACTORS[0])
            return preproducts[0] + preproducts + preproducts[-1]
        if self.state == "reverse":
            self.wear(WEAR_FACTORS[1])
            return preproducts[::-1]
        if self.state == "chop":
            self.wear(WEAR_FACTORS[2])
            if len(preproducts) > 1:
                return preproducts[:-1]
            else:
                return preproducts
        if self.state == "trim":
            self.wear(WEAR_FACTORS[3])
            if len(preproducts) > 2:
                return preproducts[1:-1]
            else:
                return preproducts
        if self.state == "split":
            self.wear(WEAR_FACTORS[4])
            middle = math.ceil(len(preproducts) / 2)
            return preproducts[:middle]
    

    def change_state(self):
        if self.state == "trim":
            self.state = "reverse"
            return
        if self.state == "reverse":
            self.state = "trim"
            return
        if self.state == "split":
            self.state = "chop"
            return
        if self.state == "chop":
            self.state = "enhance"
            return
        if self.state == "enhance":
            self.state = "split"
            return

    
    def wear(self, wear_factor):
        self.accumulated_wear += wear_factor
        if self.accumulated_wear >= THRESHOLD:
            cost = self.calculate_cost(wear_factor)
            message = self.prepare_message(cost)
            self.message_master("maintenance", message)
            self.reset_wear()
    

    def calculate_cost(self, wear_factor):
        return (self.accumulated_wear - THRESHOLD + 1) * wear_factor
    

    def prepare_message(self, cost):
        return f"{self.node_id}-{cost}-{self.current_production_cycle}"
    

    def message_master(self, message_type, message):  #TODO notify master with non-blocking send
        data = json.dumps({'type': message_type, 'message': message, 'node_id': str(self.node_id), 'cycle': self.current_production_cycle})
        self.parent_comm.isend(data, dest=0)


    def reset_wear(self):
        self.accumulated_wear = 0

    
    def increment_production_cycle(self):
        self.current_production_cycle += 1
    

    def send_product(self, product):
        if self.parent_id:
            self.local_comm.send(json.dumps({'product': product, 'node_id': int(self.node_id)}), dest=self.parent_id)
        else:
            self.message_master("result", product)
    

def main():
    parent_comm = MPI.Comm.Get_parent()
    local_comm = MPI.COMM_WORLD
    local_rank = local_comm.Get_rank()
    local_size = local_comm.Get_size()
    
    if local_rank == 0:
        exit(0)
        
    json_str = parent_comm.recv(source=0)
    node_data = json.loads(json_str)

    global NUM_PRODUCTION_CYCLES
    NUM_PRODUCTION_CYCLES = node_data['num_production_cycles']

    global WEAR_FACTORS
    WEAR_FACTORS = node_data['wear_factors']

    global THRESHOLD
    THRESHOLD = node_data['treshold']
    
    production_node = ProductionNode(local_rank,
                                     node_data['parent_id'],
                                     node_data['children'],
                                     node_data['init_state'],
                                     node_data['init_product'],
                                     parent_comm,
                                     local_comm
                                     )

    production_node.start_cycles()
        

if __name__ == '__main__':
    main()