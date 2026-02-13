package inventory

import (
	"context"
	"fmt"

	cniv1alpha1 "github-vcf.devops.broadcom.net/vcf/kubernetes-service/apis/addonconfigs/cni/v1alpha1"
	"github.com/vmware-tanzu/nsx-operator/pkg/nsx/services/inventory"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vmv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
)

func watchAntreaConfig(c *InventoryController, mgr ctrl.Manager) error {
	antreaConfigInformer, err := mgr.GetCache().GetInformer(context.Background(), &cniv1alpha1.AntreaConfig{})
	if err != nil {
		log.Error(err, "Failed to create AntreaConfig informer")
		return err
	}

	_, err = antreaConfigInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			// Handle AntreaConfig add event
			c.handleAntreaConfig(obj)
		},
		UpdateFunc: func(oldObj, newObj any) {
			// Handle AntreaConfig update event
			c.handleAntreaConfig(newObj)
		},
		DeleteFunc: func(obj any) {
			// Handle AntreaConfig delete event
			c.handleAntreaConfig(obj)
		},
	})
	if err != nil {
		log.Error(err, "Failed to add AntreaConfig event handler")
		return err
	}
	return nil
}

func (c *InventoryController) handleAntreaConfig(obj any) {
	var antreaConfig *cniv1alpha1.AntreaConfig
	ok := false
	switch obj1 := obj.(type) {
	case *cniv1alpha1.AntreaConfig:
		antreaConfig = obj1
	case cache.DeletedFinalStateUnknown:
		antreaConfig, ok = obj1.Obj.(*cniv1alpha1.AntreaConfig)
		if !ok {
			err := fmt.Errorf("obj is not valid *cniv1alpha1.AntreaConfig")
			log.Error(err, "DeletedFinalStateUnknown Obj is not *cniv1alpha1.AntreaConfig")
			return
		}
	}
	log.Debug("Inventory processing AntreaConfig", "namespace", antreaConfig.Namespace, "name", antreaConfig.Name)

	// Don't queue the vms for tagging if it's not using antrea-interworking
	if antreaConfig.Spec.AntreaNSX.Enable == nil || !*antreaConfig.Spec.AntreaNSX.Enable {
		return
	}

	var clusterName string
	for _, ref := range antreaConfig.OwnerReferences {
		if ref.Kind == "Cluster" {
			clusterName = ref.Name
			break
		}
	}

	if clusterName == "" {
		return
	}

	vms := &vmv1alpha5.VirtualMachineList{}
	if err := c.Client.List(context.Background(), vms, client.MatchingFields{inventory.ClusterNameIndexName: clusterName}); err != nil {
		log.Error(err, "Failed to get VirtualMachineList", "Namespace", antreaConfig.Namespace)
		return
	}

	for _, vm := range vms.Items {
		key := vm.Namespace + "/" + vm.Name
		log.Debug("Adding VirtualMachine key to inventory object queue", "VirtualMachine key", key)
		// We can not use the VirtualMachine resource's UID as the external ID, they do not match.
		c.inventoryObjectQueue.Add(inventory.InventoryKey{InventoryType: inventory.VirtualMachine, ExternalId: string(vm.Spec.InstanceUUID), Key: key})
	}
}
