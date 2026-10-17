package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/temporal-sa/temporal-nexus-openai-batch/internal/model"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
)

type cleanupHandle struct {
	client.ActivityHandle
	terminated bool
}

type failingActivityVisibilityClient struct{ *mocks.Client }

func (c failingActivityVisibilityClient) ListActivities(context.Context, client.ListActivitiesOptions) (client.ListActivitiesResult, error) {
	return client.ListActivitiesResult{}, serviceerror.NewUnavailable("activity visibility unavailable")
}

func (h *cleanupHandle) Terminate(context.Context, client.TerminateActivityOptions) error {
	h.terminated = true
	return nil
}

func TestCleanupUsesDurableInventoryWhenVisibilityFails(t *testing.T) {
	var c mocks.Client
	unavailable := serviceerror.NewUnavailable("visibility unavailable")
	c.On("ListWorkflow", mock.Anything, mock.Anything).Return(nil, unavailable).Once()
	c.On("CancelWorkflow", mock.Anything, "batch/run-00000001", "").Return(nil).Once()
	h := &cleanupHandle{}
	c.On("GetActivityHandle", client.GetActivityHandleOptions{ActivityID: "request-activity/run/request", RunID: "execution"}).Return(h).Once()
	err := cleanup(context.Background(), failingActivityVisibilityClient{&c}, "run", []string{"run-00000001"}, []model.CompletionTarget{{ActivityID: "request-activity/run/request", ActivityRunID: "execution"}})
	if err == nil || !h.terminated {
		t.Fatalf("cleanup must process tracked resources and report supplemental errors: terminated=%t error=%v", h.terminated, err)
	}
	c.AssertExpectations(t)
}
