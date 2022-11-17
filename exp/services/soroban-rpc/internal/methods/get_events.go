package methods

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
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

type EventInfo struct {
	Ledger         string         `json:"ledger"`
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
	StartLedger string             `json:"startLedger"`
	EndLedger   string             `json:"endLedger"`
	Filters     []EventFilter      `json:"filters"`
	Pagination  *PaginationOptions `json:"pagination,omitempty"`
}

func (g *GetEventsRequest) Valid() error {
	// Validate start & end ledger
	// TODO: Parse ledgers and enforce max range here.

	// Validate filters
	if len(g.Filters) > 5 {
		return errors.New("maximum 5 filters per request")
	}
	for i, filter := range g.Filters {
		if err := filter.Valid(); err != nil {
			return errors.Wrapf(err, "invalid filter %d", i)
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
	for _, filter := range g.Filters {
		if filter.Matches(event) {
			return true
		}
	}
	return false
}

type EventFilter struct {
	ContractIDs []string `json:"contractIds"`
	Topics      []string `json:"topics"`
}

func (e *EventFilter) Valid() error {
	if len(e.ContractIDs) > 5 {
		return errors.New("maximum 5 contract IDs per filter")
	}
	if len(e.Topics) < 1 {
		return errors.New("topic must have at least one segment")
	}
	if len(e.Topics) > 4 {
		return errors.New("topic cannot have more than 4 segments")
	}
	return nil
}

func (e *EventFilter) Matches(event xdr.ContractEvent) bool {
	// TODO: Implement this more efficiently (ideally do it in the real data backend)
	return true
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
	// TODO: Use a more efficient backend here. For now, we stream all ledgers in
	// the range from horizon, and filter them. This sucks.
	// TODO: Repeated requests to paginate through all results up to limit.
	cursor := request.StartLedger // TODO: Convert this to a txn cursor
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

	var results []EventInfo
	for transactionIndex, transaction := range transactions.Embedded.Records {
		metaBase64 := transaction.ResultMetaXdr
		metaBytes, err := base64.URLEncoding.DecodeString(metaBase64)
		if err != nil {
			// Invalid meta back. Eek!
			// TODO: Better error handling here
			return nil, err
		}
		var meta xdr.TransactionMeta
		if _, err := xdr.Unmarshal(bytes.NewReader(metaBytes), &meta); err != nil {
			// Invalid meta back. Eek!
			// TODO: Better error handling here
			return nil, err
		}

		v3, ok := meta.GetV3()
		if !ok {
			continue
		}

		ledger := fmt.Sprint(transaction.Ledger)
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
					seg, err := xdrMarshalBase64(segment)
					if err != nil {
						return nil, err
					}
					topic = append(topic, seg)
				}

				// base64-xdr encode the data
				data, err := xdrMarshalBase64(v0.Data)
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

	return results, nil
}

// TODO: Is there an off-the-shelf way to do this?
func xdrMarshalBase64(src interface{}) (string, error) {
	var buf bytes.Buffer
	_, err := xdr.Marshal(&buf, src)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(buf.Bytes()), nil
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
