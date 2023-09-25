
// TODO: WRITE TEST FOR THIS FUNCTION
func MergeEventChannels(channelSize int, chans ...<-chan *EventMessage) <-chan *EventMessage {
	mergedCh := make(chan *EventMessage, channelSize)

	go func(mergedChan chan<- *EventMessage, chans []<-chan *EventMessage) {
		messages := make([]*EventMessage, len(chans))
		for i := range messages {
			messages[i] = <-chans[i]
		}
		for {
			minIndex := -1
			minTime := time.Unix(1<<62, 0) // initialize with maximum time 
			for i := range messages {
				if messages[i].kafkaMessage.Timestamp.Before(minTime) {
					minIndex = i
					minTime = messages[i].kafkaMessage.Timestamp
				}
			}

			mergedChan <- messages[minIndex]
			messages[minIndex] = <-chans[minIndex]

		}
	}(mergedCh, chans)
	return mergedCh
}

// TODO: WRITE TEST FOR THIS FUNCTION
func MergeEventChannelsWithoutWaitForAllChannels(channelSize int, chans ...<-chan *EventMessage) <-chan *EventMessage {
	mergedCh := make(chan *EventMessage, channelSize)

	go func(mergedChan chan<- *EventMessage, chans []<-chan *EventMessage) {
		messages := make([]*EventMessage, len(chans))
		cases := make([]reflect.SelectCase, len(chans))
		for i, ch := range chans {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
		}

		for {
			isAllNil := true
			minIndex := -1
			minTime := time.Unix(1<<62, 0) // initialize with maximum time 
			for i := range messages {

				if messages[i] == nil {
					if len(chans[i]) == 0 {
						continue
					}
					messages[i] = <-chans[i]
				}
				isAllNil = false
				if messages[i].kafkaMessage.Timestamp.Before(minTime) {
					minIndex = i
					minTime = messages[i].kafkaMessage.Timestamp
				}
			}

			if !isAllNil {
				mergedChan <- messages[minIndex]
				messages[minIndex] = nil

			} else {
				chosen, value, _ := reflect.Select(cases)
				messages[chosen] = value.Interface().(*EventMessage)
			}

		}
	}(mergedCh, chans)
	return mergedCh
}
