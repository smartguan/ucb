//Student Name: Zeyuan Seth Guan
//SID# 22789869
//Login: ee122-dp
//ee122-Project1


1. My name: Zeyuan Seth Guan

2. This is my first time writing Python code, a lot of changing, but fun and beneficial

3. Extra Credits:
    a. Variable size Sliding Window: 
         The code uses a dynamic list for the storage of the packets being sent by the 
       self.send method and only update the bottom packet(the first packet being 
       stored) when an ACT is received. 
         When the self.receive method receives a Not None response, the code increases the 
       window size by 1; When the self.receive receives a None response, the code decreases
       the window size by 1.
         This implementation isn't very obvious with small data but should be significant 
       when processing large data.
    b. Accounting for variable round-trip times: 
         Update the max_wait_time by increasing it when self.receive reeceives a None 
       response and decreasing it when self.receive receives a Not None response.
         This method reduces the total time usage of the transmission.