/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/lock"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const resourceID = "resource_xxx"

func TestStandaloneRedisLock_InitError(t *testing.T) {
	t.Run("error when connection fail", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = "127.0.0.1"
		cfg.Properties["redisPassword"] = ""

		// init
		err := comp.InitLockStore(context.Background(), cfg)
		assert.Error(t, err)
	})

	t.Run("error when no host", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = ""
		cfg.Properties["redisPassword"] = ""

		// init
		err := comp.InitLockStore(context.Background(), cfg)
		assert.Error(t, err)
	})

	t.Run("error when wrong MaxRetries", func(t *testing.T) {
		// construct component
		comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
		defer comp.Close()

		cfg := lock.Metadata{Base: metadata.Base{
			Properties: make(map[string]string),
		}}
		cfg.Properties["redisHost"] = "127.0.0.1"
		cfg.Properties["redisPassword"] = ""
		cfg.Properties["maxRetries"] = "1 "

		// init
		err := comp.InitLockStore(context.Background(), cfg)
		assert.Error(t, err)
	})
}

func TestStandaloneRedisLock_Lock(t *testing.T) {
	// 0. prepare
	// start redis
	s, err := miniredis.Run()
	assert.NoError(t, err)
	comp := NewStandaloneRedisLock(logger.NewLogger("test")).(*StandaloneRedisLock)
	// construct component
	defer s.Close()
	defer comp.Close()

	cfg := lock.Metadata{Base: metadata.Base{
		Properties: make(map[string]string),
	}}
	cfg.Properties["redisHost"] = s.Addr()
	cfg.Properties["redisPassword"] = ""
	// init
	err = comp.InitLockStore(context.Background(), cfg)
	assert.NoError(t, err)

	t.Run("should unlock resource with same owner", func(t *testing.T) {
		owner := uuid.New().String()
		resp, err := comp.TryLock(context.Background(), &lock.TryLockRequest{
			ResourceID:      resourceID,
			LockOwner:       owner,
			ExpiryInSeconds: 10,
		})
		assert.NoError(t, err)
		assert.True(t, resp.Success)

		unlockResp, err := comp.Unlock(context.Background(), &lock.UnlockRequest{
			ResourceID: resourceID,
			LockOwner:  owner,
		})
		assert.NoError(t, err)
		assert.True(t, unlockResp.Status == 0, "client1 failed to unlock!")
	})

	t.Run("shouldn't unlock resource with different owners", func(t *testing.T) {
		owner := uuid.New().String()
		differentOwner := uuid.New().String()
		resource := uuid.New().String()

		resp, err := comp.TryLock(context.Background(), &lock.TryLockRequest{
			ResourceID:      resource,
			LockOwner:       owner,
			ExpiryInSeconds: 10,
		})
		assert.NoError(t, err)
		assert.True(t, resp.Success)

		unlockResp, err := comp.Unlock(context.Background(), &lock.UnlockRequest{
			ResourceID: resource,
			LockOwner:  differentOwner,
		})
		assert.NoError(t, err)
		assert.True(t, unlockResp.Status == lock.LockBelongsToOthers)
	})

	t.Run("shouldn't unlock resource if it doesnt exists", func(t *testing.T) {
		owner := uuid.New().String()
		resource := uuid.New().String()

		unlockResp, err := comp.Unlock(context.Background(), &lock.UnlockRequest{
			ResourceID: resource,
			LockOwner:  owner,
		})
		assert.NoError(t, err)
		assert.True(t, unlockResp.Status == lock.LockDoesNotExist)
	})

	t.Run("should lock with TryLock", func(t *testing.T) {
		owner := uuid.New().String()
		otherOwner := uuid.New().String()
		resource := uuid.New().String()

		resp, err := comp.TryLock(context.Background(), &lock.TryLockRequest{
			ResourceID:      resource,
			LockOwner:       owner,
			ExpiryInSeconds: 10,
		})
		assert.NoError(t, err)
		assert.True(t, resp.Success)

		// try to lock again
		resp, err = comp.TryLock(context.Background(), &lock.TryLockRequest{
			ResourceID:      resource,
			LockOwner:       otherOwner,
			ExpiryInSeconds: 10,
		})
		assert.NoError(t, err)

		// Because the resource is already locked
		assert.False(t, resp.Success)
	})

	t.Run("should acquire lock", func(t *testing.T) {
		owner := uuid.New().String()
		resource := uuid.New().String()

		var wg sync.WaitGroup
		wg.Add(1)

		go acquireLockHelper(&acquireLockHelperRequest{
			comp:     comp,
			resource: resource,
			owner:    owner,
			wg:       &wg,
			expires:  10,
			t:        t,
		})

		wg.Wait()
	})

	t.Run("should acquire lock after key is expired", func(t *testing.T) {
		owner := uuid.New().String()
		resource := uuid.New().String()

		var wg sync.WaitGroup
		wg.Add(2)

		// Acquire lock
		acquireLockHelper(&acquireLockHelperRequest{
			comp:     comp,
			resource: resource,
			owner:    owner,
			wg:       &wg,
			expires:  30,
			t:        t,
		})

		// Acquire lock again with blocking
		go acquireLockHelper(&acquireLockHelperRequest{
			comp:     comp,
			resource: resource,
			owner:    owner,
			wg:       &wg,
			expires:  30,
			t:        t,
		})

		// Unlocking by fast forwarding time
		s.FastForward(15 * time.Second)

		// Try not blocking lock to see if it can be acquired, because the key is not expired yet
		tryLockResponse, err := comp.TryLock(context.Background(), &lock.TryLockRequest{
			ResourceID:      resource,
			LockOwner:       owner,
			ExpiryInSeconds: 60,
		})

		assert.NoError(t, err)
		assert.False(t, tryLockResponse.Success)

		s.FastForward(16 * time.Second)

		// This should be unblocked now
		wg.Wait()
	})

	t.Run("should acquire lock after key is unlocked", func(t *testing.T) {
		owner := uuid.New().String()
		resource := uuid.New().String()

		var wg sync.WaitGroup
		wg.Add(3)

		// Acquire lock
		acquireLockHelper(&acquireLockHelperRequest{
			comp:     comp,
			resource: resource,
			owner:    owner,
			wg:       &wg,
			expires:  30,
			t:        t,
		})

		// Acquire lock again with blocking
		go acquireLockHelper(&acquireLockHelperRequest{
			comp:     comp,
			resource: resource,
			owner:    owner,
			wg:       &wg,
			expires:  30,
			t:        t,
		})

		go func() {
			resp, err := comp.Unlock(context.Background(), &lock.UnlockRequest{
				ResourceID: resource,
				LockOwner:  owner,
			})
			wg.Done()
			assert.NoError(t, err)
			assert.True(t, resp.Status == lock.Success)
		}()

		// This unblocks just after the lock has unlocked manually
		wg.Wait()
	})
}

type acquireLockHelperRequest struct {
	t        *testing.T
	wg       *sync.WaitGroup
	resource string
	owner    string
	comp     *StandaloneRedisLock
	expires  int32
}

func acquireLockHelper(req *acquireLockHelperRequest) {
	// This will be blocked until the lock is released
	// Then it acquires the new lock
	resp, err := req.comp.Lock(context.Background(), &lock.LockRequest{
		ResourceID:      req.resource,
		LockOwner:       req.owner,
		ExpiryInSeconds: req.expires,
	})
	assert.NoError(req.t, err)
	assert.True(req.t, resp.Success)
	req.wg.Done()
}
