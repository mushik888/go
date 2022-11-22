package methods

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/code"
	"github.com/creachadair/jrpc2/handler"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/toid"
	"github.com/stellar/go/xdr"
)

// MAX_LEDGER_RANGE is the maximum allowed value of endLedger-startLedger
// TODO: Pick and document a max here. Paul just guessed 4320 as it is ~6hrs
const MAX_LEDGER_RANGE = 4320

type EventInfo struct {
	Ledger         int32          `json:"ledger,string"`
	LedgerClosedAt string         `json:"ledgerClosedAt"`
	ContractID     string         `json:"contractId"`
	ID             string         `json:"id"`
	PagingToken    string         `json:"pagingToken"`
	Topic          []string       `json:"topic"`
	Value          EventInfoValue `json:"value"`
}

type EventInfoValue struct {
	XDR string `json:"xdr"`
}

type GetEventsRequest struct {
	StartLedger int32              `json:"startLedger,string"`
	EndLedger   int32              `json:"endLedger,string"`
	Filters     []EventFilter      `json:"filters"`
	Pagination  *PaginationOptions `json:"pagination,omitempty"`
}

func (g *GetEventsRequest) Valid() error {
	// Validate start & end ledger
	// Validate the ledger range min/max
	if g.EndLedger < g.StartLedger {
		return errors.New("endLedger must be after or the same as startLedger")
	}
	if g.EndLedger-g.StartLedger > MAX_LEDGER_RANGE {
		return fmt.Errorf("endLedger must be less than %d ledgers after startLedger", MAX_LEDGER_RANGE)
	}

	// Validate filters
	if len(g.Filters) > 5 {
		return errors.New("maximum 5 filters per request")
	}
	for i, filter := range g.Filters {
		if err := filter.Valid(); err != nil {
			return errors.Wrapf(err, "filter %d invalid", i+1)
		}
	}

	return nil
}

func (g *GetEventsRequest) Matches(event xdr.ContractEvent) bool {
	if event.Type != xdr.ContractEventTypeContract {
		// TODO: Should we handle system events? or just contract ones?
		return false
	}
	if event.ContractId == nil {
		// TODO: again, system events?
		return false
	}
	if len(g.Filters) == 0 {
		return true
	}
	for _, filter := range g.Filters {
		if filter.Matches(event) {
			return true
		}
	}
	return false
}

type EventFilter struct {
	ContractIDs []string      `json:"contractIds"`
	Topics      []TopicFilter `json:"topics"`
}

func (e *EventFilter) Valid() error {
	if len(e.ContractIDs) > 5 {
		return errors.New("maximum 5 contract IDs per filter")
	}
	if len(e.Topics) > 5 {
		return errors.New("maximum 5 topics per filter")
	}
	for i, id := range e.ContractIDs {
		out, err := hex.DecodeString(id)
		if err != nil || len(out) != 32 {
			return fmt.Errorf("contract ID %d invalid", i+1)
		}
	}
	for i, topic := range e.Topics {
		if err := topic.Valid(); err != nil {
			return errors.Wrapf(err, "topic %d invalid", i+1)
		}
	}
	return nil
}

// TODO: Implement this more efficiently (ideally do it in the real data backend)
func (e *EventFilter) Matches(event xdr.ContractEvent) bool {
	return e.matchesContractIDs(event) && e.matchesTopics(event)
}

func (e *EventFilter) matchesContractIDs(event xdr.ContractEvent) bool {
	if len(e.ContractIDs) == 0 {
		return true
	}
	if event.ContractId == nil {
		return false
	}
	needle := hex.EncodeToString((*event.ContractId)[:])
	for _, id := range e.ContractIDs {
		if id == needle {
			return true
		}
	}
	return false
}

func (e *EventFilter) matchesTopics(event xdr.ContractEvent) bool {
	if len(e.Topics) == 0 {
		return true
	}
	v0 := event.Body.MustV0()
	for _, topicFilter := range e.Topics {
		if topicFilter.Matches(v0.Topics) {
			return true
		}
	}
	return false
}

type TopicFilter []SegmentFilter

func (t *TopicFilter) Valid() error {
	if len(*t) < 1 {
		return errors.New("topic must have at least one segment")
	}
	if len(*t) > 4 {
		return errors.New("topic cannot have more than 4 segments")
	}
	return nil
}

func (t TopicFilter) Matches(event []xdr.ScVal) bool {
	for _, segmentFilter := range t {
		if segmentFilter.wildcard != nil {
			switch *segmentFilter.wildcard {
			case "*":
				// one-segment wildcard
				if len(event) == 0 {
					// Nothing to match, need one segment.
					return false
				}
				// Ignore this token
				event = event[1:]
			default:
				panic("invalid segmentFilter")
			}
		} else if segmentFilter.scval != nil {
			// Exact match the scval
			if len(event) == 0 || !segmentFilter.scval.Equals(event[0]) {
				return false
			}
			event = event[1:]
		} else {
			panic("invalid segmentFilter")
		}
	}
	// Check we had no leftovers
	return len(event) == 0
}

type SegmentFilter struct {
	wildcard *string
	scval    *xdr.ScVal
}

func (s *SegmentFilter) UnmarshalJSON(p []byte) error {
	s.wildcard = nil
	s.scval = nil

	var tmp string
	if err := json.Unmarshal(p, &tmp); err != nil {
		return err
	}
	if tmp == "*" {
		s.wildcard = &tmp
	} else {
		var out xdr.ScVal
		if err := xdr.SafeUnmarshalBase64(tmp, &out); err != nil {
			return err
		}
		s.scval = &out
	}
	return nil
}

type PaginationOptions struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  uint   `json:"limit,omitempty"`
}

type EventStore struct {
	Client *horizonclient.Client
}

func (a EventStore) GetEvents(request GetEventsRequest) ([]EventInfo, error) {
	if err := request.Valid(); err != nil {
		return nil, err
	}

	var results []EventInfo

	// TODO: Use a more efficient backend here. For now, we stream all ledgers in
	// the range from horizon, and filter them. This sucks.
	cursor := toid.New(request.StartLedger, 0, 0).String()
	for {
		transactions, err := a.Client.Transactions(horizonclient.TransactionRequest{
			Order:         horizonclient.Order("asc"),
			Cursor:        cursor,
			Limit:         200,
			IncludeFailed: false,
		})
		if err != nil {
			// TODO: Better error handling/retry here
			return nil, err
		}

		if len(transactions.Embedded.Records) == 0 {
			// No transactions found??
			return nil, fmt.Errorf("no transactions found at cursor: %s", cursor)
		}

		for transactionIndex, transaction := range transactions.Embedded.Records {
			if transaction.Ledger > request.EndLedger {
				return results, nil
			}
			cursor = transaction.PagingToken()
			var meta xdr.TransactionMeta
			if err := xdr.SafeUnmarshalBase64(transaction.ResultMetaXdr, &meta); err != nil {
				// Invalid meta back. Eek!
				// TODO: Better error handling here
				return nil, err
			}

			v3, ok := meta.GetV3()
			if !ok {
				continue
			}

			ledger := transaction.Ledger
			ledgerClosedAt := transaction.LedgerCloseTime.Format(time.RFC3339)

			// TODO: Handle nested events list once
			// https://github.com/stellar/stellar-xdr/pull/52 is merged. For now the
			// operationIndex here is a placeholder. There is only one operation for
			// now, so we can use that assumption to build the event id correctly.
			operationIndex := 0

			for eventIndex, event := range v3.Events {
				if request.Matches(event) {
					v0 := event.Body.MustV0()

					// Build a lexically order-able id for this event record. This is
					// based on Horizon's db2/history.Effect.ID method.
					id := fmt.Sprintf(
						"%019d-%010d",
						toid.New(
							transaction.Ledger,
							int32(transactionIndex+1),
							int32(operationIndex+1),
						),
						eventIndex+1,
					)

					// base64-xdr encode the topic
					topic := make([]string, 4)
					for _, segment := range v0.Topics {
						seg, err := xdr.MarshalBase64(segment)
						if err != nil {
							return nil, err
						}
						topic = append(topic, seg)
					}

					// base64-xdr encode the data
					data, err := xdr.MarshalBase64(v0.Data)
					if err != nil {
						return nil, err
					}

					results = append(results, EventInfo{
						Ledger:         ledger,
						LedgerClosedAt: ledgerClosedAt,
						ContractID:     hex.EncodeToString((*event.ContractId)[:]),
						ID:             id,
						PagingToken:    id,
						Topic:          topic,
						Value:          EventInfoValue{XDR: data},
					})
				}
			}
		}
	}
}

// NewGetEventsHandler returns a json rpc handler to fetch and filter events
func NewGetEventsHandler(store EventStore) jrpc2.Handler {
	return handler.New(func(ctx context.Context, request GetEventsRequest) ([]EventInfo, error) {
		response, err := store.GetEvents(request)
		if err != nil {
			if herr, ok := err.(*horizonclient.Error); ok {
				return response, (&jrpc2.Error{
					Code:    code.InvalidRequest,
					Message: herr.Problem.Title,
				}).WithData(herr.Problem.Extras)
			}
			return response, err
		}
		return response, nil
	})
}
