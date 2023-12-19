from mpi4py import MPI
import json

class ProductionNode():
    def __init__(self, node_id, parent_id, children, init_state, init_product=None):
        self.node_id = node_id
        self.parent_id = parent_id
        self.children = children
        self.state = init_state
        self.init_product = init_product
    
    def produce(self, preproducts):
        return '|'.join(preproducts)


def main():
    parent_comm = MPI.Comm.Get_parent()
    local_comm = MPI.COMM_WORLD
    local_rank = local_comm.Get_rank()
    local_size = local_comm.Get_size()
    
    if local_rank == 0:
        exit(0)
        
    json_str = parent_comm.recv(source=0)
    node_data = json.loads(json_str)
    
    production_node = ProductionNode(local_rank, node_data['parent_id'], node_data['children'], node_data['init_state'], node_data['init_product'])
    
    
    # print(f"------SLAVE {local_rank}------")
    # print(json.dumps(production_node.__dict__, indent=4))
    # print(node_data)
    # print("---------------------------")
    
    preproducts = []
    if production_node.children:
        for child_id in production_node.children:
            product_of_child = local_comm.recv(source=child_id)
            preproducts.append(product_of_child)
    elif production_node.init_product:
        preproducts.append(production_node.init_product)
        
    product = production_node.produce(preproducts)
    print(f"Slave {local_rank} produced: {product}")
    
    if production_node.parent_id is not None:
        local_comm.send(product, dest=production_node.parent_id)
        
    
        
        
        
        
    
    


if __name__ == '__main__':
    main()