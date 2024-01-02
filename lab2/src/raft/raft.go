/*
*RAFT 
 */
 package raft

 import (
	 "labrpc"
	 "math/rand"
	 "sync"
	 "sync/atomic"
	 "time"
 )
 
 type RequestVoteArgs struct {
	 // Your data here (2A, 2B).
	 CandidateTerm   int
	 CandidateID     int
	 LastLogPosition int
	 LogTermIndex    int
 }
 
 type RequestVoteReply struct {
	 // Your data here (2A).
	 CurrentCandidateTerm int
	 VoteApproved         bool
 }
 
 type AppendEntriesArgs struct {
	 LeaderTerm int // leader's term (2A)
 
	 OldIndexEntry     int
	 OldTermlog        int
	 NewEntries        []LogEntry
	 CommitIndexLeader int
 }
 
 type AppendEntriesReply struct {
	 CurrTerm          int
	 Aprroved          bool
	 DisagreementPoint int
 }
 
 type ApplyMsg struct {
	 CommandValid bool
	 Command      interface{}
	 CommandIndex int
 }
 
 const (
	 StateFollower  = 0
	 StateCandidate = 1
	 StateLeader    = 2
 )
 
 type LogEntry struct {
	 RaftCommand interface{}
	 RaftTerm    int
 }
 
 type Raft struct {
	 mu        sync.Mutex          // Lock to protect shared access to this peer's serverState
	 peers     []*labrpc.ClientEnd // RPC end points of all peers
	 persister *Persister          // Object to hold this peer's persisted serverState
	 me        int                 // this peer's index into peers[]
	 dead      int32               // set by Kill()
 
	 // Your data here (2A, 2B, 2C).
	 // Look at the paper's Figure 2 for a description of what
 
	 electionTimer       chan bool
	 hbtimerChannel      chan bool
	 seenTerm            int
	 electionVote        int
	 serverState         int32
	 hbTimeout           int64
	 electionDelay       int64
	 ResetelectlastTimer int64
	 lasthbTimeout       int64
	 appliedIndex        int
	 commitLevel         int
	 commitChannel       chan ApplyMsg
	 logEntries          []LogEntry
	 forwardingIndex     []int
	 replIndex           []int
 }
 
 func (rf *Raft) GetState() (term int, isleader bool) {
 
	 // Your code here (2A).
	 rf.mu.Lock()
	 defer rf.mu.Unlock()
 
	 term = rf.seenTerm
	 isleader = rf.serverState == StateLeader
	 return term, isleader
 }
 func (rf *Raft) dispatchAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	 return rf.peers[server].Call("Raft.AppendEntries", args, reply)
 }
 func (rf *Raft) commitToStateMachine() {
	 rf.mu.Lock()
	 defer rf.mu.Unlock()
 
	 for rf.appliedIndex < rf.commitLevel {
		 rf.appliedIndex++
		 applyMsg := ApplyMsg{
			 CommandValid: true,
			 Command:      rf.logEntries[rf.appliedIndex].RaftCommand,
			 CommandIndex: rf.appliedIndex,
		 }
		 rf.commitChannel <- applyMsg
 
	 }
 }
 
 func (rf *Raft) persist() {
	 // Your code here (2C).
 
 }
 
 // restore previously persisted serverState.
 func (rf *Raft) readPersist(data []byte) {
	 if data == nil || len(data) < 1 { // bootstrap without any serverState?
		 return
	 }
	 // Your code here (2C).
 
 }
 
 func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
 
	 ///2A
	 ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	 return ok
 }
 
 func (rf *Raft) Start(command interface{}) (int, int, bool) {
	 index := -1
	 term := -1
	 isLeader := true
 
	 // Your code here (2B).
 
	 term, isLeader = rf.GetState()
	 if !isLeader {
		 return index, term, false // Early return if not leader
	 }
 
	 rf.mu.Lock()
	 rf.logEntries = append(rf.logEntries, LogEntry{command, rf.seenTerm})
	 rf.persist()
	 rf.replIndex[rf.me] = rf.forwardingIndex[rf.me] + len(rf.logEntries) - 1
	 index = len(rf.logEntries) - 1
 
	 rf.mu.Unlock()
 
	 return index, term, isLeader
 }
 
 // example RequestVote RPC handler.
 func (rf *Raft) RequestVoteCase1(args *RequestVoteArgs, reply *RequestVoteReply) {
	 //called from RequestVote()
 
	 if rf.seenTerm < args.CandidateTerm {
		 rf.setState(StateFollower)
		 rf.seenTerm = args.CandidateTerm
		 rf.persist()
	 }
 }
 func (rf *Raft) RequestVoteCase2(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	 //called from RequestVote()
 
	 if args.CandidateTerm < rf.seenTerm || (rf.electionVote != -1 && rf.electionVote != args.CandidateID) {
		 reply.CurrentCandidateTerm = rf.seenTerm
		 reply.VoteApproved = false
		 return false
 
	 }
	 return true
 }
 
 func (rf *Raft) RequestVoteCase3(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	 //called from RequestVote()
 
	 lastIndex := len(rf.logEntries) - 1
	 lastTerm := rf.logEntries[lastIndex].RaftTerm
	 if lastTerm > args.LogTermIndex || (lastTerm == args.LogTermIndex && lastIndex > args.LastLogPosition) {
		 reply.CurrentCandidateTerm = rf.seenTerm
		 reply.VoteApproved = false
		 return false
	 }
 
	 return true
 
 }
 
 func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	 // 2A and 2B
	 rf.mu.Lock()
	 defer rf.mu.Unlock()
 
	 // Case 1: Candidate has a higher term. Convert to follower and update term.
 
	 rf.RequestVoteCase1(args, reply)
 
	 if rf.RequestVoteCase2(args, reply) == false {
		 return
	 }
 
	 // Case 3: Grant vote if the candidate's log is at least as up-to-date as this node's log.
	 if rf.RequestVoteCase3(args, reply) == false {
		 return
	 }
 
	 // Case 4: If the log checks out, grant the vote.
	 reply.CurrentCandidateTerm = rf.seenTerm
	 reply.VoteApproved = true
	 rf.electionVote = args.CandidateID
	 rf.persist()
	 rf.refreshElectionTimer()
 }
 
 func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	 rf.mu.Lock()
	 defer rf.mu.Unlock()
 
	 if !rf.case1CheckTerm(args, reply) {
		 return
	 }
 
	 // Case 2: Update term and convert to follower if leader's term is newer.
	 rf.case2UpdateState(args)
 
	 //Case 3: Reject if log is inconsistent with leader's.
	 lastIndex := len(rf.logEntries)
	 if lastIndex <= args.OldIndexEntry || rf.logEntries[args.OldIndexEntry].RaftTerm != args.OldTermlog {
		 reply.CurrTerm = rf.seenTerm
		 reply.Aprroved = false
		 reply.DisagreementPoint = lastIndex
		 if lastIndex > args.OldIndexEntry {
			 for i := args.OldIndexEntry; i > 0; i-- {
				 if rf.logEntries[i].RaftTerm != rf.logEntries[i-1].RaftTerm {
					 reply.DisagreementPoint = i
					 break
				 }
			 }
		 }
		 return
	 }
 
	 // Case 4: Append any new entries not already in the log.
	 rf.case4AppendNewEntries(args)
 
	 // Case 5: Update commit index if leader's commit index is greater.
	 newLastIndex := args.OldIndexEntry + len(args.NewEntries)
	 if args.CommitIndexLeader > rf.commitLevel {
		 rf.commitLevel = getMin(args.CommitIndexLeader, newLastIndex)
		 go rf.commitToStateMachine()
	 }
 
	 reply.CurrTerm = rf.seenTerm
	 reply.Aprroved = true
 }
 func getMin(val1, val2 int) int {
	 if val1 > val2 {
		 return val2
	 }
	 return val1
 }
 
 func (rf *Raft) case1CheckTerm(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	 //called from Appendentries func
	 if args.LeaderTerm < rf.seenTerm {
		 reply.CurrTerm = rf.seenTerm
		 reply.Aprroved = false
		 return false // Early exit, leader term is outdated.
	 }
	 return true
 }
 func (rf *Raft) case2UpdateState(args *AppendEntriesArgs) {
	 //called from Appendentries func
	 //reset election timer
	 rf.refreshElectionTimer()
 
	 if args.LeaderTerm > rf.seenTerm {
		 rf.setState(StateFollower)
		 rf.seenTerm = args.LeaderTerm
		 rf.persist()
	 } else if rf.serverState == StateCandidate {
		 rf.serverState = StateFollower
	 }
 
 }
 func (rf *Raft) case4AppendNewEntries(args *AppendEntriesArgs) {
	 //called from Appendentries func
	 lastIndex := len(rf.logEntries)
	 matchIndex := args.OldIndexEntry + 1
	 for i, newEntry := range args.NewEntries {
		 if matchIndex+i >= lastIndex || rf.logEntries[matchIndex+i].RaftTerm != newEntry.RaftTerm {
			 rf.logEntries = append(rf.logEntries[:matchIndex+i], args.NewEntries[i:]...)
			 rf.persist()
			 break
		 }
	 }
 
 }
 
 func (rf *Raft) startElection() {
	 rf.mu.Lock()
	 rf.setState(StateCandidate)
	 rf.persist()
	 totalVotes := 1
	 rf.mu.Unlock()
 
	 for i := range rf.peers {
 
		 if i != rf.me {
			 go func(id int) {
				 prevLog := len(rf.logEntries) - 1
				 args := RequestVoteArgs{
					 CandidateTerm:   rf.seenTerm,
					 CandidateID:     rf.me,
					 LastLogPosition: prevLog,
					 LogTermIndex:    rf.logEntries[prevLog].RaftTerm,
				 }
 
				 var response RequestVoteReply
				 if rf.sendRequestVote(id, &args, &response) {
					 rf.mu.Lock()
					 if rf.seenTerm == args.CandidateTerm {
 
						 if response.VoteApproved {
							 totalVotes++
							 if totalVotes > len(rf.peers)/2 && rf.serverState == StateCandidate {
								 rf.setState(StateLeader)
								 for i := range rf.peers {
									 rf.forwardingIndex[i] = prevLog + 1
									 rf.replIndex[i] = 0
								 }
								 rf.mu.Unlock()
								 rf.maintainLeadership()
								 return
							 }
						 } else if rf.seenTerm < response.CurrentCandidateTerm {
							 rf.setState(StateFollower)
							 rf.seenTerm = response.CurrentCandidateTerm
							 rf.persist()
						 }
 
					 }
					 rf.mu.Unlock()
 
				 }
			 }(i)
		 }
	 }
 }
 
 func (rf *Raft) maintainLeadership() {
	 if !rf.checkIfLeader() {
		 return
	 }
 
	 rf.resetTimerHeartbeat()
 
	 for i := range rf.peers {
		 if i != rf.me {
			 go rf.keepAlive(i)
		 }
	 }
 }
 
 // checkIfLeader checks if the current server is the leader.
 func (rf *Raft) checkIfLeader() bool {
	 //maintainLeadership()
	 rf.mu.Lock()
	 defer rf.mu.Unlock()
 
	 return rf.serverState == StateLeader
 }
 
 // keepAlive handles the logic of sending a heartbeat to a single peer.
 func (rf *Raft) keepAlive(peerID int) {
	 //maintainLeadership()
	 for {
		 if !rf.sendAppendEntriesWrapper(peerID) {
			 return
		 }
	 }
 }
 
 // sendAppendEntriesWrapper wraps the dispatchAppendEntries call with the necessary state.
 func (rf *Raft) sendAppendEntriesWrapper(peerID int) bool {
	 //maintainLeadership()
	 rf.mu.Lock()
	 term := rf.seenTerm
	 leaderCommit := rf.commitLevel
	 nextIndex := rf.forwardingIndex[peerID]
	 prevLogIndex := nextIndex - 1
	 prevLogTerm := rf.logEntries[prevLogIndex].RaftTerm
	 entries := rf.logEntries[nextIndex:]
	 rf.mu.Unlock()
 
	 // Prepare the arguments for the append entries RPC.
	 args := AppendEntriesArgs{
		 LeaderTerm:        term,
		 OldIndexEntry:     prevLogIndex,
		 OldTermlog:        prevLogTerm,
		 NewEntries:        entries,
		 CommitIndexLeader: leaderCommit,
	 }
 
	 reply := AppendEntriesReply{}
	 if ok := rf.dispatchAppendEntries(peerID, &args, &reply); !ok {
		 return false
	 }
 
	 return rf.handleAppendEntriesResponse(peerID, &args, &reply)
 }
 
 // handleAppendEntriesResponse handles the response from the append entries RPC.
 func (rf *Raft) handleAppendEntriesResponse(peerID int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	 //maintainLeadership()
	 rf.mu.Lock()
	 defer rf.mu.Unlock()
 
	 if rf.serverState != StateLeader || rf.seenTerm != args.LeaderTerm {
		 return false
	 }
 
	 if reply.Aprroved {
		 rf.replIndex[peerID] = args.OldIndexEntry + len(args.NewEntries)
		 rf.forwardingIndex[peerID] = rf.replIndex[peerID] + 1
		 temp := len(rf.logEntries) - 1
		 for index := temp; index > rf.commitLevel && rf.logEntries[index].RaftTerm == rf.seenTerm; index-- {
			 count := 0
			 for i := 0; i < len(rf.peers); i++ {
				 if rf.replIndex[i] >= index && rf.logEntries[index].RaftTerm == rf.seenTerm {
					 count += 1
				 }
				 if count > len(rf.peers)/2 {
					 rf.commitLevel = index
					 go rf.commitToStateMachine()
					 break
				 }
			 }
		 }
 
		 return false
	 }
 
	 if reply.CurrTerm > rf.seenTerm {
		 rf.setState(StateFollower)
		 rf.seenTerm = reply.CurrTerm
		 rf.persist()
		 return false
	 }
 
	 rf.forwardingIndex[peerID] = reply.DisagreementPoint
	 return true
 }
 
 func (rf *Raft) setState(state int32) {
	 if state == StateFollower {
		 rf.electionVote = -1
		 rf.serverState = StateFollower
	 } else if state == StateCandidate {
		 rf.serverState = StateCandidate
		 rf.seenTerm++
		 rf.electionVote = rf.me
		 rf.refreshElectionTimer()
	 } else if state == StateLeader {
		 rf.serverState = StateLeader
		 rf.resetTimerHeartbeat()
	 }
 }
 func (rf *Raft) leaderHeartbeatMonitor() {
	 for {
		 rf.mu.Lock()
		 if rf.serverState == StateLeader && ((time.Now().UnixNano()-rf.lasthbTimeout)/time.Hour.Milliseconds()) > rf.hbTimeout {
			 DPrintf("Timeout::: current term: %d ::: current serverState: %d\n", rf.seenTerm, rf.serverState)
			 rf.hbtimerChannel <- true
		 }
		 rf.mu.Unlock()
		 time.Sleep(time.Millisecond * 5)
	 }
 }
 func (rf *Raft) scheduleElection() {
	 for {
		 rf.mu.Lock()
 
		 if rf.serverState != StateLeader && ((time.Now().UnixNano()-rf.ResetelectlastTimer)/time.Hour.Milliseconds() > rf.electionDelay) {
			 {
				 DPrintf("election timeout d\n")
				 rf.electionTimer <- true
			 }
		 }
		 rf.mu.Unlock()
		 time.Sleep(time.Millisecond * 5)
	 }
 }
 
 func (rf *Raft) Kill() {
	 atomic.StoreInt32(&rf.dead, 1)
 }
 
 func Make(peers []*labrpc.ClientEnd, me int,
	 persister *Persister, applyCh chan ApplyMsg) *Raft {
	 rf := &Raft{}
	 rf.peers = peers
	 rf.persister = persister
	 rf.me = me
 
	 // Your initialization code here (2A, 2B, 2C).
	 rf.serverState = StateFollower
	 rf.seenTerm = 0
	 rf.electionVote = -1
	 rf.hbtimerChannel = make(chan bool)
	 rf.electionTimer = make(chan bool)
	 rf.hbTimeout = 10 // ms
	 rf.refreshElectionTimer()
	 rf.logEntries = append(rf.logEntries, LogEntry{RaftTerm: 0})
	 rf.forwardingIndex = make([]int, len(rf.peers))
	 rf.replIndex = make([]int, len(rf.peers))
	 rf.commitLevel = 0
	 rf.appliedIndex = 0
	 rf.commitChannel = applyCh
 
	 // initialize from serverState persisted before a crash
	 rf.readPersist(persister.ReadRaftState())
 
	 go rf.consensusCycle()
	 go rf.scheduleElection()
	 go rf.leaderHeartbeatMonitor()
 
	 return rf
 }
 
 func (rf *Raft) resetTimerHeartbeat() {
	 rf.lasthbTimeout = time.Now().UnixNano()
 }
 func (rf *Raft) consensusCycle() {
	 for atomic.LoadInt32(&rf.dead) != 1 {
		 // Non-blocking check of election timer
		 select {
		 case <-rf.electionTimer:
			 rf.startElection()
		 default:
		 }
		 // Non-blocking check of heartbeat timer channel
		 select {
		 case <-rf.hbtimerChannel:
			 rf.maintainLeadership()
		 default:
		 }
		 // Sleep for a short duration to avoid spinning too fast
		 time.Sleep(time.Millisecond * 5)
	 }
 }
 func (rf *Raft) refreshElectionTimer() {
	 rand.Seed(time.Now().UnixNano())
	 rf.electionDelay = rf.hbTimeout*2 + rand.Int63n(100)
	 rf.ResetelectlastTimer = time.Now().UnixNano()
 }
 