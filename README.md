# Raft

```puml
title Leader

start

:Create a No-Op log entry;
note right
    To ensure logs from previous term are commited.
end note

repeat

fork

:Get log from client or No-Op from startup;

partition "For each peer" {
    repeat
        :Send AppendEntries;
        if (Rejected) then (Y)
            :Decrease log index to send;
        endif
    repeat while (success) is (N)
}

:If majority ACK;
:Increase commited Index;
:Respond to client;
fork again

if (Idle for X seconds) then (Y)
    :Send Empty AppendEntries; 
endif

fork again

:Apply commited log entries;

fork again

if (Receive AppendEntries With Higher Term) then (Y)
    :Convert To Follower;
    end
endif

end fork
```

```puml
title Follower

start
repeat

fork

:Accept log entries from leader;
:Apply commited log entries;

fork again

if (Receive RequestVote) then (Y)
    if (Req.Term > my Term ||\n(\n  Req.Term == my Term &&\n  Req.LogIndex > my Log Index\n)) then (Y)
        :Grant vote;
    else (N)
        :Reject;
    endif
endif

fork again

if (Leader timeout) then (Y)
    :Convert to Candidate;
    end
endif

end fork
```

```puml
title Candidate

start

:Term++;
:Vote for self;

repeat

:Wait random time;

fork

if (Get majority votes) then (Y)
    :Convert to leader;
    end
else (Timeout)
endif

fork again

if (Receive AppendEntries && req.Term >= my Term) then
    :Convert to follower;
    end
endif

end fork
```
