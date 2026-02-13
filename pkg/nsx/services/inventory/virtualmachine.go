package inventory

import (
	"context"
	"fmt"

	cniv1alpha1 "github-vcf.devops.broadcom.net/vcf/kubernetes-service/apis/addonconfigs/cni/v1alpha1"
	"github.com/antihax/optional"
	"github.com/vmware/go-vmware-nsxt/common"
	"github.com/vmware/go-vmware-nsxt/manager"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	vmv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
)

const (
	ClusterNameLabelKey string = "capv.vmware.com/cluster.name"

	supervisorIDAnnotationKey string = "vmoperator.vmware.com/manager-id"

	virtualMachineTagKey string = "nsx-op/vks-cluster-id"
)

func (s *InventoryService) initVirtualMachines() error {
	cursor := ""
	log.Info("Retrieving VirtualMachines for cluster")
	for {
		opts := map[string]interface{}{}
		if cursor != "" {
			opts["cursor"] = optional.NewString(cursor)
		}
		virtualMachines, _, err := s.NSXClient.NsxApiClient.FabricApi.ListVirtualMachines(context.Background(), opts)
		if err != nil {
			return fmt.Errorf("failed to retrieve VirtualMachines err: %w", err)
		}
		for _, vm := range virtualMachines.Results {
			var tag string
			for _, vmTag := range vm.Tags {
				if vmTag.Scope == virtualMachineTagKey {
					tag = vmTag.Tag
					break
				}
			}

			if tag == "" {
				// Not tagged
				continue
			}

			err = s.VirtualMachineStore.Add(&VirtualMachineObj{
				ExternalId: vm.ExternalId,
				Tag:        tag,
			})
			if err != nil {
				return err
			}
		}
		if cursor = virtualMachines.Cursor; cursor == "" {
			break
		}
	}
	return nil
}

func (s *InventoryService) SyncVirtualMachine(name string, namespace string, key InventoryKey) *InventoryKey {
	externalId := key.ExternalId
	var tags []common.Tag

	vmLink := s.VirtualMachineStore.GetByKey(externalId)
	if vmLink != nil { // Already Tagged
		return nil
	}

	vm := &vmv1alpha5.VirtualMachine{}
	err := s.Client.Get(context.TODO(), types.NamespacedName{Name: name, Namespace: namespace}, vm)
	if err != nil {
		if apierrors.IsNotFound(err) {
			if err := s.DeleteResource(externalId, VirtualMachine); err != nil {
				log.Error(err, "Delete VirtualMachine Resource error", "key", key)
				return &key
			}
		} else {
			log.Error(err, "Unexpected error is found while processing VirtualMachine")
		}
		return nil
	}

	clusterName, ok := vm.Labels[ClusterNameLabelKey]
	if !ok {
		log.Info("Skipping VM, not owned by VKS", "key", key)
		return nil
	}

	supervisorID, ok := vm.Annotations[supervisorIDAnnotationKey]
	if !ok {
		log.Error(fmt.Errorf("missing supervisor id"), "Unable to generate tag for VirtualMachine", "key", key)
		return nil
	}

	if vm.Status.PowerState != vmv1alpha5.VirtualMachinePowerStateOn {
		log.Info("Skipping VM, powerState is not poweredOn")
		return nil
	}

	// TODO: Maybe we should select by cluster name via labels
	antreaConfigName := fmt.Sprintf("%s-antrea-package", clusterName)
	antreaConfig := &cniv1alpha1.AntreaConfig{}
	if err := s.Client.Get(context.Background(), types.NamespacedName{Name: antreaConfigName, Namespace: namespace}, antreaConfig); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Skipping VM, not using Antrea CNI")
			return nil
		}

		log.Error(err, "Failed to get AntreaConfig for cluster", "name", antreaConfigName, "cluster", clusterName)
		return &key
	}

	if antreaConfig.Spec.AntreaNSX.Enable == nil || !*antreaConfig.Spec.AntreaNSX.Enable {
		log.Info("Skipping VM, antrea-interworking may not be installed on cluster")
		return nil
	}

	tag := fmt.Sprintf("%s-%s-%s-antrea", supervisorID, namespace, clusterName)
	tags = append(tags, common.Tag{
		Scope: virtualMachineTagKey,
		Tag:   tag,
	})

	// TODO: We may want to ensure we don't clobber any existing tags
	// Get the existing VM inventory object and only add to it if it
	// doesn't already have the tag
	if _, err = s.NSXClient.NsxApiClient.FabricApi.UpdateVirtualMachineTagsUpdateTags(
		context.Background(),
		manager.VirtualMachineTagUpdate{
			ExternalId: externalId,
			Tags:       tags,
		},
	); err != nil {
		log.Error(err, "Failed to update tag for VirtualMachine inventory", "key", key)
		return &key
	}

	s.pendingAdd[externalId] = &VirtualMachineObj{
		ExternalId: externalId,
		Tag:        tag,
	}

	log.Info("Successfully updated tags for vm", "vm", vm.Name)
	return nil
}
