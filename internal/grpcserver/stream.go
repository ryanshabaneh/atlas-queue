// internal/grpcserver/stream.go
package grpcserver

import (
	"encoding/json"

	pb "github.com/yourorg/atlas/proto"
)

type jobEventPayload struct {
	JobID           string `json:"job_id"`
	State           string `json:"state"`
	StateVersion    int32  `json:"state_version"`
	CancelRequested bool   `json:"cancel_requested"`
}

// StreamJobUpdates listens on the PostgreSQL job_events NOTIFY channel and
// forwards matching events to the gRPC stream. The stream is best-effort:
// pg_notify does not guarantee delivery. Clients use state_version to detect
// gaps. An empty job_id in the request means "all jobs".
func (s *Server) StreamJobUpdates(
	req *pb.StreamRequest,
	stream pb.AtlasQueue_StreamJobUpdatesServer,
) error {
	conn, err := s.Pool.Acquire(stream.Context())
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(stream.Context(), "LISTEN job_events"); err != nil {
		return err
	}

	for {
		notif, err := conn.Conn().WaitForNotification(stream.Context())
		if err != nil {
			if stream.Context().Err() != nil {
				return nil
			}
			return err
		}

		var event jobEventPayload
		if err := json.Unmarshal([]byte(notif.Payload), &event); err != nil {
			continue
		}

		if req.JobId != "" && event.JobID != req.JobId {
			continue
		}

		if err := stream.Send(&pb.JobEvent{
			JobId:           event.JobID,
			State:           event.State,
			StateVersion:    event.StateVersion,
			CancelRequested: event.CancelRequested,
		}); err != nil {
			return err
		}
	}
}
