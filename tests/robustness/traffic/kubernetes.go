// Copyright 2023 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package traffic

import (
	"context"
	"fmt"
	"math/rand"

	"golang.org/x/time/rate"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/stringutil"
	"go.etcd.io/etcd/tests/v3/robustness/identity"
)

var (
	KubernetesTraffic = Config{
		Name:        "Kubernetes",
		minimalQPS:  200,
		maximalQPS:  1000,
		clientCount: 12,
		traffic: kubernetesTraffic{
			averageKeyCount: 5,
			resource:        "pods",
			namespace:       "default",
			writeChoices: []choiceWeight[KubernetesRequestType]{
				{choice: KubernetesUpdate, weight: 75},
				{choice: KubernetesDelete, weight: 15},
				{choice: KubernetesCreate, weight: 10},
			},
		},
	}
)

type kubernetesTraffic struct {
	averageKeyCount int
	resource        string
	namespace       string
	writeChoices    []choiceWeight[KubernetesRequestType]
}

type KubernetesRequestType string

const (
	KubernetesUpdate KubernetesRequestType = "update"
	KubernetesCreate KubernetesRequestType = "create"
	KubernetesDelete KubernetesRequestType = "delete"
)

func (t kubernetesTraffic) Run(ctx context.Context, clientId int, c *RecordingClient, limiter *rate.Limiter, ids identity.Provider, lm identity.LeaseIdStorage, finish <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-finish:
			return
		default:
		}
		objects, err := t.Range(ctx, c, "/registry/"+t.resource+"/", true)
		if err != nil {
			continue
		}
		limiter.Wait(ctx)
		err = t.Write(ctx, c, ids, objects)
		if err != nil {
			continue
		}
		limiter.Wait(ctx)
	}
}

func (t kubernetesTraffic) Write(ctx context.Context, c *RecordingClient, ids identity.Provider, objects []*mvccpb.KeyValue) (err error) {
	writeCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	if len(objects) < t.averageKeyCount/2 {
		err = t.Create(writeCtx, c, t.generateKey(), fmt.Sprintf("%d", ids.NewRequestId()))
	} else {
		randomPod := objects[rand.Intn(len(objects))]
		if len(objects) > t.averageKeyCount*3/2 {
			err = t.Delete(writeCtx, c, string(randomPod.Key), randomPod.ModRevision)
		} else {
			op := KubernetesRequestType(pickRandom(t.writeChoices))
			switch op {
			case KubernetesDelete:
				err = t.Delete(writeCtx, c, string(randomPod.Key), randomPod.ModRevision)
			case KubernetesUpdate:
				err = t.Update(writeCtx, c, string(randomPod.Key), fmt.Sprintf("%d", ids.NewRequestId()), randomPod.ModRevision)
			case KubernetesCreate:
				err = t.Create(writeCtx, c, t.generateKey(), fmt.Sprintf("%d", ids.NewRequestId()))
			default:
				panic(fmt.Sprintf("invalid choice: %q", op))
			}
		}
	}
	cancel()
	return err
}

func (t kubernetesTraffic) generateKey() string {
	return fmt.Sprintf("/registry/%s/%s/%s", t.resource, t.namespace, stringutil.RandString(5))
}

func (t kubernetesTraffic) Range(ctx context.Context, c *RecordingClient, key string, withPrefix bool) ([]*mvccpb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	resp, err := c.Range(ctx, key, withPrefix)
	cancel()
	return resp, err
}

func (t kubernetesTraffic) Create(ctx context.Context, c *RecordingClient, key, value string) error {
	return t.Update(ctx, c, key, value, 0)
}

func (t kubernetesTraffic) Update(ctx context.Context, c *RecordingClient, key, value string, expectedRevision int64) error {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	err := c.CompareRevisionAndPut(ctx, key, value, expectedRevision)
	cancel()
	return err
}

func (t kubernetesTraffic) Delete(ctx context.Context, c *RecordingClient, key string, expectedRevision int64) error {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	err := c.CompareRevisionAndDelete(ctx, key, expectedRevision)
	cancel()
	return err
}
