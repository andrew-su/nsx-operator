package inventory

import (
	"context"
	"fmt"

	"github.com/vmware-tanzu/nsx-operator/pkg/nsx/services/inventory"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vmv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
)

func watchVirtualMachine(c *InventoryController, mgr ctrl.Manager) error {
	mgr.GetFieldIndexer().IndexField(context.Background(), &vmv1alpha5.VirtualMachine{}, inventory.ClusterNameIndexName, func(o client.Object) []string {
		clusterName, ok := o.GetLabels()[inventory.ClusterNameLabelKey]
		if !ok {
			return nil
		}
		return []string{clusterName}
	})

	vmInformer, err := mgr.GetCache().GetInformer(context.Background(), &vmv1alpha5.VirtualMachine{})
	if err != nil {
		log.Error(err, "Failed to create VirtualMachine informer")
		return err
	}

	_, err = vmInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			// Handle VirtualMachine add event
			c.handleVirtualMachine(obj)
		},
		UpdateFunc: func(oldObj, newObj any) {
			// Handle VirtualMachine update event
			c.handleVirtualMachine(newObj)
		},
		DeleteFunc: func(obj any) {
			// Handle VirtualMachine delete event
			c.handleVirtualMachine(obj)
		},
	})
	if err != nil {
		log.Error(err, "Failed to add VirtualMachine event handler")
		return err
	}
	return nil
}

func (c *InventoryController) handleVirtualMachine(obj any) {
	var vm *vmv1alpha5.VirtualMachine
	ok := false
	switch obj1 := obj.(type) {
	case *vmv1alpha5.VirtualMachine:
		vm = obj1
	case cache.DeletedFinalStateUnknown:
		vm, ok = obj1.Obj.(*vmv1alpha5.VirtualMachine)
		if !ok {
			err := fmt.Errorf("obj is not valid *vmv1alpha5.VirtualMachine")
			log.Error(err, "DeletedFinalStateUnknown Obj is not *vmv1alpha5.VirtualMachine")
			return
		}
	}
	log.Debug("Inventory processing VirtualMachine", "namespace", vm.Namespace, "name", vm.Name)
	key, _ := keyFunc(vm)
	log.Debug("Adding VirtualMachine key to inventory object queue", "VirtualMachine key", key)
	// We can not use the VirtualMachine resource's UID as the external ID, they do not match.
	c.inventoryObjectQueue.Add(inventory.InventoryKey{InventoryType: inventory.VirtualMachine, ExternalId: string(vm.Spec.InstanceUUID), Key: key})
}
