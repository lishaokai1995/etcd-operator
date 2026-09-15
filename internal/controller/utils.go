package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ecv1alpha1 "go.etcd.io/etcd-operator/api/v1alpha1"
	"go.etcd.io/etcd-operator/pkg/certificate"
	certInterface "go.etcd.io/etcd-operator/pkg/certificate/interfaces"
)

const (
	etcdDataDir = "/var/lib/etcd"
	volumeName  = "etcd-data"

	// DefaultImageRegistry is the fallback container image registry used by
	// the CLI flag default and by tests that wire an EtcdClusterReconciler.
	DefaultImageRegistry = "gcr.io/etcd-development/etcd"
)

type etcdClusterState string

const (
	etcdClusterStateNew      etcdClusterState = "new"
	etcdClusterStateExisting etcdClusterState = "existing"
)

// pvcNameForMember returns the PVC name for a given pod, matching the naming
// convention that StatefulSet VolumeClaimTemplates would have produced.
func pvcNameForMember(podName string) string {
	return fmt.Sprintf("%s-%s", volumeName, podName)
}

// etcdClusterLabels returns the label set applied to every member pod and used
// by the headless Service selector.
func etcdClusterLabels(ec *ecv1alpha1.EtcdCluster) map[string]string {
	return map[string]string{
		"app":        ec.Name,
		"controller": ec.Name,
	}
}

// clusterNameLabel marks cluster-owned objects with their EtcdCluster's name,
// letting owned objects be selected by label.
const clusterNameLabel = "operator.etcd.io/cluster"

// memberOrdinalLabel distinguishes each member Pod within a cluster by its
// ordinal, letting per-member Services (e.g. NodePort exposure of a single
// member) select exactly one Pod. Unlike etcdClusterLabels it is unique per
// member and is applied automatically on every Pod build, so it survives
// Pod recreation without manual kubectl labeling.
const memberOrdinalLabel = "operator.etcd.io/member-ordinal"

// etcdMemberLabels returns the per-member label set added to each member Pod
// on top of etcdClusterLabels, keyed by the member's ordinal.
func etcdMemberLabels(ordinal int) map[string]string {
	return map[string]string{memberOrdinalLabel: strconv.Itoa(ordinal)}
}

// clusterNameLabels returns the label set that marks objects (EtcdMembers,
// PVCs) with the name of the cluster they belong to, letting them be
// selected by label.
func clusterNameLabels(clusterName string) map[string]string {
	return map[string]string{clusterNameLabel: clusterName}
}

// createPVCForMember creates a PVC for the given pod if one does not already
// exist.  Naming mirrors StatefulSet VolumeClaimTemplates: "{volumeName}-{podName}".
// The PVC is owned by the EtcdMember and labeled with the cluster name so
// per-cluster PVCs can be listed by label selector.
func createPVCForMember(ctx context.Context, c client.Client, ec *ecv1alpha1.EtcdCluster, member *ecv1alpha1.EtcdMember, podName string, scheme *runtime.Scheme) error {
	pvcName := pvcNameForMember(podName)

	existing := &corev1.PersistentVolumeClaim{}
	err := c.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: ec.Namespace}, existing)
	if err == nil {
		return nil // already exists
	}
	if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to check PVC %s: %w", pvcName, err)
	}

	volumeSizeLimit := ec.Spec.StorageSpec.VolumeSizeLimit
	if volumeSizeLimit.IsZero() {
		volumeSizeLimit = ec.Spec.StorageSpec.VolumeSizeRequest
	}

	accessMode := ec.Spec.StorageSpec.AccessModes
	if accessMode == "" {
		accessMode = corev1.ReadWriteOnce
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: ec.Namespace,
			Labels:    clusterNameLabels(ec.Name),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{accessMode},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: ec.Spec.StorageSpec.VolumeSizeRequest,
				},
				Limits: corev1.ResourceList{
					corev1.ResourceStorage: volumeSizeLimit,
				},
			},
		},
	}

	if ec.Spec.StorageSpec.StorageClassName != "" {
		pvc.Spec.StorageClassName = &ec.Spec.StorageSpec.StorageClassName
	}

	if err := controllerutil.SetControllerReference(member, pvc, scheme); err != nil {
		return err
	}

	return c.Create(ctx, pvc)
}

func RemoveStringFromSlice(s []string, str string) []string {
	for i := range s {
		defaultArg := getArgName(s[i])
		if defaultArg == str {
			s = slices.Delete(s, i, i+1)
			break
		}
	}
	return s
}

func getArgName(s string) string {
	idx := strings.Index(s, "=")
	if idx != -1 {
		return s[:idx]
	}
	idx = strings.Index(s, " ")
	if idx != -1 {
		return s[:idx]
	}
	return strings.TrimSpace(s)
}

func createArgs(name string, etcdOptions []string, tlsEnabled bool) []string {
	defaultArgs := defaultArgs(name, tlsEnabled)
	if len(etcdOptions) > 0 {
		for i := range etcdOptions {
			argName := getArgName(etcdOptions[i])
			defaultArgs = RemoveStringFromSlice(defaultArgs, argName)
		}
	}
	defaultArgs = append(defaultArgs, etcdOptions...)
	return defaultArgs
}

// ---------------------------------------------------------------------------
// Kubernetes resource helpers
// ---------------------------------------------------------------------------

func createHeadlessServiceIfNotExist(ctx context.Context, logger logr.Logger, c client.Client, ec *ecv1alpha1.EtcdCluster, scheme *runtime.Scheme) error {
	service := &corev1.Service{}
	err := c.Get(ctx, client.ObjectKey{Name: ec.Name, Namespace: ec.Namespace}, service)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("Headless service does not exist. Creating headless service")
			headlessSvc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ec.Name,
					Namespace: ec.Namespace,
					Labels:    etcdClusterLabels(ec),
				},
				Spec: corev1.ServiceSpec{
					ClusterIP: "None",
					Selector:  etcdClusterLabels(ec),
					// PublishNotReadyAddresses keeps each member's Pod IP in the
					// headless Service's DNS A record set even while the Pod is
					// NotReady (e.g. a newly created member whose etcd process is
					// still bootstrapping). etcd v3.6's peer-port TLS handler runs
					// checkCertSAN, which forward-DNS-resolves the cert's DNSName
					// and checks the connecting Pod IP against the returned A
					// records. Without this flag CoreDNS returns only Ready Pod
					// IPs, so a new member's own IP is absent from the lookup
					// result, every peer handshake it initiates is rejected
					// ("tls: \"<ip>\" does not match any of DNSNames"), its etcd
					// bootstrap dies, and the Pod stays NotReady forever — a
					// chicken-and-egg deadlock during cluster scale-out.
					PublishNotReadyAddresses: true,
				},
			}
			if err := controllerutil.SetControllerReference(ec, headlessSvc, scheme); err != nil {
				return err
			}
			if createErr := c.Create(ctx, headlessSvc); createErr != nil {
				return fmt.Errorf("failed to create headless service: %w", createErr)
			}
			logger.Info("Headless service created successfully")
			return nil
		}
		return fmt.Errorf("failed to get headless service: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// External access (per-member NodePort Services)
// ---------------------------------------------------------------------------

// externalServiceName returns the NodePort Service name for one member.
func externalServiceName(clusterName string, ordinal int) string {
	return fmt.Sprintf("%s-%d-external", clusterName, ordinal)
}

// externalServiceLabels returns the label set for a member's external Service:
// the cluster identity labels (so it can be found by cluster) plus the
// per-member ordinal label.
func externalServiceLabels(ec *ecv1alpha1.EtcdCluster, ordinal int) map[string]string {
	labels := etcdClusterLabels(ec)
	for k, v := range etcdMemberLabels(ordinal) {
		labels[k] = v
	}
	return labels
}

// reconcileExternalAccess converges the cluster's per-member NodePort Services
// to the desired state derived from Spec.ExternalAccess and Spec.Size:
//   - disabled (nil or Enabled=false): deletes every existing external Service
//     and clears s.externalAccessPorts.
//   - enabled: ensures one Service per ordinal in [0, Size), each selecting
//     exactly its member Pod via the operator.etcd.io/member-ordinal label, and
//     deletes any Service whose ordinal is now out of range (scale-down).
//
// The NodePort observed on each live Service is collected into
// s.externalAccessPorts for updateStatus to surface. Runs unconditionally every
// reconcile alongside the other cluster prerequisites.
func (r *EtcdClusterReconciler) reconcileExternalAccess(ctx context.Context, logger logr.Logger, s *reconcileState) error {
	ec := s.cluster
	ns := ec.Namespace

	// List every external Service this cluster already owns, so both the
	// enable and disable paths can reconcile against it and prune orphans.
	existing := &corev1.ServiceList{}
	if err := r.List(ctx, existing,
		client.InNamespace(ns),
		client.MatchingLabels(etcdClusterLabels(ec)),
	); err != nil {
		return fmt.Errorf("failed to list external services: %w", err)
	}

	cfg := ec.Spec.ExternalAccess
	if cfg == nil || !cfg.Enabled {
		// Disabled: delete all owned external Services, then clear the report.
		for i := range existing.Items {
			if !isExternalService(&existing.Items[i]) {
				continue
			}
			if err := r.Delete(ctx, &existing.Items[i]); err != nil && !k8serrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete external service %s: %w", existing.Items[i].Name, err)
			}
			logger.Info("Deleted external access Service", "service", existing.Items[i].Name)
		}
		s.externalAccessPorts = nil
		return nil
	}

	port := cfg.Port
	if port == 0 {
		port = 2379
	}

	// Track which ordinals are still desired so we can prune the rest.
	desired := make(map[int]struct{}, ec.Spec.Size)
	ports := make([]ecv1alpha1.ExternalAccessPortStatus, 0, ec.Spec.Size)

	for ordinal := 0; ordinal < ec.Spec.Size; ordinal++ {
		desired[ordinal] = struct{}{}
		name := externalServiceName(ec.Name, ordinal)

		svc := &corev1.Service{}
		err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, svc)
		if k8serrors.IsNotFound(err) {
			svc = newExternalService(ec, ordinal, port, cfg.NodePort)
			if err := controllerutil.SetControllerReference(ec, svc, r.Scheme); err != nil {
				return fmt.Errorf("failed to set owner reference on external service %s: %w", name, err)
			}
			if err := r.Create(ctx, svc); err != nil {
				if k8serrors.IsAlreadyExists(err) {
					// Another reconcile created it concurrently; re-fetch below.
					if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, svc); err != nil {
						return fmt.Errorf("failed to get external service %s after create race: %w", name, err)
					}
				} else {
					return fmt.Errorf("failed to create external service %s: %w", name, err)
				}
			} else {
				logger.Info("Created external access Service", "service", name)
			}
		} else if err != nil {
			return fmt.Errorf("failed to get external service %s: %w", name, err)
		} else {
			// Exists: reconcile the port spec, preserving the cluster-assigned
			// NodePort (never re-set it, or the API server rejects the change).
			if updateExternalServicePorts(svc, port) {
				if err := r.Update(ctx, svc); err != nil {
					return fmt.Errorf("failed to update external service %s: %w", name, err)
				}
				logger.Info("Updated external access Service", "service", name)
			}
		}

		ports = append(ports, ecv1alpha1.ExternalAccessPortStatus{
			Ordinal:  int32(ordinal),
			NodePort: nodePortOf(svc, cfg.NodePort, ordinal),
		})
	}

	// Prune external Services whose ordinal is no longer desired (scale-down or
	// a name that changed). Only touch Services we recognize as ours.
	for i := range existing.Items {
		svc := &existing.Items[i]
		if !isExternalService(svc) {
			continue
		}
		ordinal, ok := ordinalFromExternalService(svc, ec.Name)
		if !ok {
			continue
		}
		if _, keep := desired[ordinal]; keep {
			continue
		}
		if err := r.Delete(ctx, svc); err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete stale external service %s: %w", svc.Name, err)
		}
		logger.Info("Deleted stale external access Service", "service", svc.Name)
	}

	slices.SortFunc(ports, func(a, b ecv1alpha1.ExternalAccessPortStatus) int {
		return int(a.Ordinal) - int(b.Ordinal)
	})
	s.externalAccessPorts = ports
	return nil
}

// newExternalService builds a per-member NodePort Service. When pinned is
// non-zero it sets an explicit nodePort (pinned+ordinal); otherwise the API
// server allocates one from the cluster's NodePort range.
func newExternalService(ec *ecv1alpha1.EtcdCluster, ordinal int, port, pinned int32) *corev1.Service {
	nodePorts := []corev1.ServicePort{
		{
			Name:       "client",
			Port:       port,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(port),
		},
	}
	if pinned != 0 {
		nodePorts[0].NodePort = pinned + int32(ordinal)
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      externalServiceName(ec.Name, ordinal),
			Namespace: ec.Namespace,
			Labels:    externalServiceLabels(ec, ordinal),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: externalServiceLabels(ec, ordinal),
			Ports:    nodePorts,
		},
	}
}

// updateExternalServicePorts aligns an existing external Service's client port
// with the desired spec and reports whether anything changed. The assigned
// NodePort is deliberately left untouched: once the API server allocates one it
// is immutable in practice, and re-setting a pinned value that drifted would be
// rejected. Port (the Service/target port) is safe to change.
func updateExternalServicePorts(svc *corev1.Service, port int32) bool {
	changed := false
	for i := range svc.Spec.Ports {
		p := &svc.Spec.Ports[i]
		if p.Port != port {
			p.Port = port
			changed = true
		}
		if p.TargetPort.IntValue() != int(port) {
			p.TargetPort = intstr.FromInt32(port)
			changed = true
		}
	}
	return changed
}

// nodePortOf returns the NodePort to report for a Service: the live value the
// API server assigned once allocated, else the pinned value, else 0 (still
// pending allocation).
func nodePortOf(svc *corev1.Service, pinned int32, ordinal int) int32 {
	for i := range svc.Spec.Ports {
		if svc.Spec.Ports[i].NodePort != 0 {
			return svc.Spec.Ports[i].NodePort
		}
	}
	if pinned != 0 {
		return pinned + int32(ordinal)
	}
	return 0
}

// isExternalService reports whether a Service is one of the operator-managed
// per-member external NodePort Services (vs the headless cluster Service).
func isExternalService(svc *corev1.Service) bool {
	_, ok := svc.Labels[memberOrdinalLabel]
	return ok && svc.Spec.Type == corev1.ServiceTypeNodePort
}

// ordinalFromExternalService extracts the member ordinal from a Service's
// operator.etcd.io/member-ordinal label.
func ordinalFromExternalService(svc *corev1.Service, clusterName string) (int, bool) {
	if svc.Labels["app"] != clusterName {
		return 0, false
	}
	v, ok := svc.Labels[memberOrdinalLabel]
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false
	}
	return n, true
}

// peerEndpointForOrdinalIndex returns the member name and peer URL for a given
// ordinal, used both to build ETCD_INITIAL_CLUSTER and to call AddMember. The
// peer URL scheme reflects the cluster's TLS configuration (https when TLS is
// configured, http otherwise).
func peerEndpointForOrdinalIndex(ec *ecv1alpha1.EtcdCluster, index int) (string, string) {
	name := fmt.Sprintf("%s-%d", ec.Name, index)
	return name, fmt.Sprintf("%s://%s-%d.%s.%s.svc.cluster.local:2380",
		clusterScheme(clusterTLSEnabled(ec)), ec.Name, index, ec.Name, ec.Namespace)
}

// getMemberNameFromPeerURL returns the {cluster}-{ordinal} member name
// encoded in a peer URL of the shape produced by peerEndpointForOrdinalIndex
// — the hostname label before its first ".". Returns "" if the URL has no
// host.
func getMemberNameFromPeerURL(peerURL string) string {
	u, err := url.Parse(peerURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if idx := strings.Index(host, "."); idx != -1 {
		return host[:idx]
	}
	return host
}

// ---------------------------------------------------------------------------
// Certificate helpers (unchanged from original implementation)
// ---------------------------------------------------------------------------

func getClientCertName(etcdClusterName string) string {
	return fmt.Sprintf("%s-%s-tls", etcdClusterName, "client")
}

func getServerCertName(etcdClusterName string) string {
	return fmt.Sprintf("%s-%s-tls", etcdClusterName, "server")
}

func getPeerCertName(etcdClusterName string) string {
	return fmt.Sprintf("%s-%s-tls", etcdClusterName, "peer")
}

func parseValidityDuration(customizedDuration string, defaultDuration time.Duration) (time.Duration, error) {
	if customizedDuration == "" {
		return defaultDuration, nil
	}
	duration, err := time.ParseDuration(customizedDuration)
	if err != nil {
		return 0, fmt.Errorf("failed to parse ValidityDuration: %w", err)
	}
	return duration, nil
}

func createCMCertificateConfig(ec *ecv1alpha1.EtcdCluster) (*certInterface.Config, error) {
	cmConfig := ec.Spec.TLS.ProviderCfg.CertManagerCfg
	if cmConfig == nil {
		return nil, fmt.Errorf("cert-manager configuration is not present")
	}

	duration, err := parseValidityDuration(cmConfig.ValidityDuration, certInterface.DefaultCertManagerValidity)
	if err != nil {
		return nil, err
	}

	var getAltNames certInterface.AltNames
	if cmConfig.AltNames.DNSNames != nil {
		getAltNames = certInterface.AltNames{
			DNSNames: cmConfig.AltNames.DNSNames,
			IPs:      cmConfig.AltNames.IPs,
		}
	} else {
		defaultDNSNames := []string{
			fmt.Sprintf("*.%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
			fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
		}
		getAltNames = certInterface.AltNames{DNSNames: defaultDNSNames}
	}

	return &certInterface.Config{
		CommonName:       cmConfig.CommonName,
		Organization:     cmConfig.Organization,
		ValidityDuration: duration,
		AltNames:         getAltNames,
		ExtraConfig: map[string]any{
			"issuerName": cmConfig.IssuerName,
			"issuerKind": cmConfig.IssuerKind,
		},
	}, nil
}

func createAutoCertificateConfig(ec *ecv1alpha1.EtcdCluster) (*certInterface.Config, error) {
	autoConfig := ec.Spec.TLS.ProviderCfg.AutoCfg
	if autoConfig == nil {
		autoConfig = &ecv1alpha1.ProviderAutoConfig{
			CommonConfig: ecv1alpha1.CommonConfig{
				CommonName:       fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
				ValidityDuration: certInterface.DefaultAutoValidity.String(),
			},
		}
	}

	duration, err := parseValidityDuration(autoConfig.ValidityDuration, certInterface.DefaultAutoValidity)
	if err != nil {
		return nil, err
	}

	var altNames certInterface.AltNames
	if autoConfig.AltNames.DNSNames != nil {
		altNames = certInterface.AltNames{
			DNSNames: autoConfig.AltNames.DNSNames,
			IPs:      autoConfig.AltNames.IPs,
		}
	} else {
		defaultDNSNames := []string{
			fmt.Sprintf("*.%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
			fmt.Sprintf("%s.%s.%s", ec.Name, ec.Namespace, certInterface.DefaultDomainName),
		}
		altNames = certInterface.AltNames{DNSNames: defaultDNSNames}
	}

	return &certInterface.Config{
		CommonName:       autoConfig.CommonName,
		Organization:     autoConfig.Organization,
		ValidityDuration: duration,
		AltNames:         altNames,
	}, nil
}

// getCertificateProvider returns the cert provider matching providerType, or
// an error if the type is unknown. An empty providerType defaults to "auto",
// matching the convention used elsewhere when no provider is set on the
// cluster spec.
func getCertificateProvider(providerType string, c client.Client) (certInterface.Provider, error) {
	if providerType == "" {
		providerType = string(certificate.Auto)
	}
	return certificate.NewProvider(certificate.ProviderType(providerType), c)
}

func createCertificate(ec *ecv1alpha1.EtcdCluster, ctx context.Context, c client.Client, certName string) error {
	providerName := ec.Spec.TLS.Provider
	cert, certErr := getCertificateProvider(providerName, c)
	if certErr != nil {
		return certErr
	}
	_, getCertError := cert.GetCertificateConfig(ctx, client.ObjectKey{Name: certName, Namespace: ec.Namespace})
	if getCertError != nil {
		if k8serrors.IsNotFound(getCertError) {
			log.Printf("Creating certificate: %s for etcd-operator: %s\n", certName, ec.Name)
			secretKey := client.ObjectKey{Name: certName, Namespace: ec.Namespace}

			switch certificate.ProviderType(providerName) {
			case certificate.Auto:
				autoConfig, err := createAutoCertificateConfig(ec)
				if err != nil {
					return fmt.Errorf("error creating auto certificate config: %w", err)
				}
				if createCertErr := cert.EnsureCertificateSecret(ctx, secretKey, autoConfig); createCertErr != nil {
					return fmt.Errorf("error creating auto certificate: %w", createCertErr)
				}
				return nil
			case certificate.CertManager:
				cmConfig, err := createCMCertificateConfig(ec)
				if err != nil {
					return fmt.Errorf("error creating cert-manager certificate config: %w", err)
				}
				if createCertErr := cert.EnsureCertificateSecret(ctx, secretKey, cmConfig); createCertErr != nil {
					return fmt.Errorf("error creating cert-manager certificate: %w", createCertErr)
				}
				return nil
			default:
				log.Printf("Error creating certificate, valid certificate provider not defined.")
				return nil
			}
		}
		return fmt.Errorf("%s:Error getting certificate", getCertError)
	}
	return nil
}

func createClientCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	certName := getClientCertName(ec.Name)
	if err := createCertificate(ec, ctx, c, certName); err != nil {
		return err
	}
	return patchCertificateSecret(ctx, ec, c, certName)
}

func createServerCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	serverCertName := getServerCertName(ec.Name)
	if err := createCertificate(ec, ctx, c, serverCertName); err != nil {
		return err
	}
	return patchCertificateSecret(ctx, ec, c, serverCertName)
}

func createPeerCertificate(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	peerCertName := getPeerCertName(ec.Name)
	if err := createCertificate(ec, ctx, c, peerCertName); err != nil {
		return err
	}
	return patchCertificateSecret(ctx, ec, c, peerCertName)
}

func applyEtcdMemberCerts(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) error {
	if ec.Spec.TLS != nil {
		if err := createServerCertificate(ctx, ec, c); err != nil {
			return err
		}
		return createPeerCertificate(ctx, ec, c)
	}
	return nil
}

// deleteClusterCertificateSecrets removes the cluster's TLS certificate
// Secrets listed in certNames via the cluster's cert provider.
//
// The actual cleanup is delegated to the cert provider: "auto" removes the
// Secret directly; "cert-manager" first deletes the Certificate CR and then
// the Secret (cert-manager does not auto-clean its Secrets). Each provider's
// DeleteCertificateSecret is NotFound-tolerant: certs that were never
// created, or were already removed, are silently skipped.
//
// Returns nil immediately if the cluster has no TLS configured.
func deleteClusterCertificateSecrets(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client, certNames []string) error {
	if ec.Spec.TLS == nil {
		return nil
	}
	provider, err := getCertificateProvider(ec.Spec.TLS.Provider, c)
	if err != nil {
		return fmt.Errorf("unknown TLS certificate provider %q: %w", ec.Spec.TLS.Provider, err)
	}
	for _, name := range certNames {
		if err := provider.DeleteCertificateSecret(ctx, client.ObjectKey{Name: name, Namespace: ec.Namespace}); err != nil {
			return fmt.Errorf("failed to delete certificate %q: %w", name, err)
		}
	}
	return nil
}

func patchCertificateSecret(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client, certSecretName string) error {
	getCertSecret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: certSecretName, Namespace: ec.Namespace}, getCertSecret); err != nil {
		return err
	}

	log.Printf("Setting ownerReference for certificate secret: %s", certSecretName)
	if err := controllerutil.SetControllerReference(ec, getCertSecret, c.Scheme()); err != nil {
		return err
	}
	if err := c.Update(ctx, getCertSecret); err != nil {
		return fmt.Errorf("failed to update certificate secret with ownerReference: %w", err)
	}
	return nil
}

// verifySecretHasCA returns an error if the supplied certificate Secret lacks
// a ca.crt data key. etcd requires --trusted-ca-file / --peer-trusted-ca-file,
// so a Secret without the CA cannot drive a TLS-enabled cluster. The auto
// provider always writes ca.crt; cert-manager only does so for CA-type Issuers
// (SelfSigned/CA), so the missing-key case surfaces an actionable error rather
// than mounting a non-existent file.
func verifySecretHasCA(secret *corev1.Secret, provider string) error {
	if _, ok := secret.Data[corev1.ServiceAccountRootCAKey]; !ok {
		// corev1.ServiceAccountRootCAKey == "ca.crt".
		return fmt.Errorf("certificate Secret %s (provider %s) is missing the ca.crt key; "+
			"for cert-manager use a SelfSigned or CA Issuer (ACME does not emit ca.crt)",
			secret.Name, provider)
	}
	return nil
}

// buildClientTLSConfig assembles the operator's TLS config from the cluster's
// server certificate Secret. The operator reuses the server identity (tls.crt/tls.key)
// and trusts the server CA (ca.crt), so the control plane can reach a TLS-enabled etcd client listener.
//
// TODO: reusing the server certificate as the operator's client identity works
// but conflates two distinct identities. Issue the operator its own dedicated
// client certificate (e.g. an "operator" cert signed by the same CA, similar
// to how createClientCertificate provisions one for external clients) instead
// of reusing getServerCertName's Secret here.
func buildClientTLSConfig(ctx context.Context, ec *ecv1alpha1.EtcdCluster, c client.Client) (*tls.Config, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, client.ObjectKey{Name: getServerCertName(ec.Name), Namespace: ec.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to get server certificate secret for client TLS: %w", err)
	}

	if err := verifySecretHasCA(secret, ec.Spec.TLS.Provider); err != nil {
		return nil, err
	}

	certData, ok := secret.Data[corev1.TLSCertKey]
	if !ok || len(certData) == 0 {
		return nil, fmt.Errorf("server certificate secret %s is missing tls.crt", secret.Name)
	}
	keyData, ok := secret.Data[corev1.TLSPrivateKeyKey]
	if !ok || len(keyData) == 0 {
		return nil, fmt.Errorf("server certificate secret %s is missing tls.key", secret.Name)
	}

	keyPair, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return nil, fmt.Errorf("failed to load server keypair for client TLS: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(secret.Data[corev1.ServiceAccountRootCAKey]) {
		return nil, fmt.Errorf("failed to parse ca.crt from server certificate secret %s", secret.Name)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{keyPair},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ---------------------------------------------------------------------------
// Version validation
// ---------------------------------------------------------------------------

// validateEtcdUpgradePath checks whether upgrading from current to target is
// permitted by the official etcd upgrade policy. If canParse is false, one of
// the version strings could not be parsed as semver.
func validateEtcdUpgradePath(etcdVersions []semver.Version, current, target string) (canParse bool, err error) {
	var (
		currentVer            *semver.Version
		targetVer             *semver.Version
		currentIdx, targetIdx = -1, -1
	)

	currentVer, err = semver.NewVersion(current)
	if err != nil {
		return false, fmt.Errorf("failed to parse current version %s: %w", current, err)
	}
	targetVer, err = semver.NewVersion(target)
	if err != nil {
		return false, fmt.Errorf("failed to parse target version %s: %w", target, err)
	}

	for idx, v := range etcdVersions {
		if v.Major == currentVer.Major && v.Minor == currentVer.Minor {
			currentIdx = idx
		}
		if v.Major == targetVer.Major && v.Minor == targetVer.Minor {
			targetIdx = idx
		}
		if currentIdx != -1 && targetIdx != -1 {
			break
		}
	}

	switch {
	case currentIdx == -1:
		return true, fmt.Errorf("unknown current version %s", currentVer)
	case targetIdx == -1:
		return true, fmt.Errorf("unknown target version %s", targetVer)
	case currentIdx > targetIdx || (currentIdx == targetIdx && currentVer.Patch > targetVer.Patch):
		return true, fmt.Errorf("downgrading from version %s to version %s is not allowed",
			currentVer, targetVer)
	case targetIdx > currentIdx+1:
		return true, fmt.Errorf("upgrading from version %s to version %s is not allowed",
			currentVer, targetVer)
	}

	return true, nil
}
