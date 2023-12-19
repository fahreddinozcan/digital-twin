from mpi4py import MPI
import json

class PrductionNode():
    def __init__(self, node_id, parent_id, children, init_state, init_product):
        self.node_id = node_id
        self.parent_id = parent_id
        self.children = children
        self.state = init_state
        self.product = init_product    


def main():
    parent_comm = MPI.Comm.Get_parent()
    local_comm = MPI.COMM_WORLD
    local_rank = local_comm.Get_rank()
    local_size = local_comm.Get_size()
    
    if local_rank == 0:
        exit(0)
        
    
        
    json_str = parent_comm.recv(source=0)
    json_obj = json.loads(json_str)
    
    print(f"------SLAVE {local_rank}------")
    print(json_obj)
    print("---------------------------")
    
    if local_rank == 5:
        local_comm.send("Hello from slave 5", dest=3)
    elif local_rank == 3:
        data = local_comm.recv(source=5)
        print(f"Slave 3 received: {data}")
        
        
    
    


if __name__ == '__main__':
    main()