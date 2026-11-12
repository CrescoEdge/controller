package io.cresco.agent.controller.netdiscovery;

import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.security.cert.Certificate;

public class DiscoveryProcessor {

	private ControllerEngine controllerEngine;
	private PluginBuilder plugin;
	private CLogger logger;

	private String verifyMessage = "DISCOVERY_MESSAGE_VERIFIED";
	private String validateMessage = "DISCOVERY_MESSAGE_VALIDATED";

	private DiscoveryCrypto discoveryCrypto;

	public DiscoveryProcessor(ControllerEngine controllerEngine) {
		this.controllerEngine = controllerEngine;
		this.plugin = controllerEngine.getPluginBuilder();
		this.logger = plugin.getLogger(DiscoveryProcessor.class.getName(), CLogger.Level.Info);
		this.discoveryCrypto = new DiscoveryCrypto(controllerEngine);
		this.warnedLegacy = WARNED.computeIfAbsent(controllerEngine, k -> java.util.concurrent.ConcurrentHashMap.newKeySet());
	}

	/**
	 * May this controller answer a discovery exchange of {@code type}? Pre-provisioned, a GLOBAL exchange is answered only by
	 * the fabric's global controller (its certificate carries cresco:role:global): every region holds the global discovery
	 * secret (it needs it to join), so the secret alone does not say which controller is the global.
	 */
	private boolean answersAs(DiscoveryType type) {
		if (type != DiscoveryType.GLOBAL) return true;
		io.cresco.agent.controller.communication.CertificateManager cm = controllerEngine.getCertificateManager();
		return cm == null || !cm.isProvisioned() || cm.isProvisionedGlobal();
	}

	/**
	 * security_profile=production: discovery runs on the operator's PKI ({@link DiscoveryAuth#AUTH_PKI}) -- both sides sign
	 * the exchange with their provisioned identity keys and their chains verify to trust_anchors -- and no discovery secret
	 * is used or accepted. Otherwise (the standard profile, provisioned or not) the discovery secret's v2 exchange.
	 */
	boolean pkiDiscovery() {
		io.cresco.agent.controller.communication.CertificateManager cm = controllerEngine.getCertificateManager();
		return cm != null && cm.isProvisioned() && io.cresco.agent.controller.communication.SecuritySettings.production(plugin.getConfig());
	}

	private java.security.cert.X509Certificate[] chainOf(String chainJson) {
		return chainJson == null ? null : controllerEngine.getCertificateManager().getCertsfromJson(chainJson);
	}

	private java.security.PrivateKey ownKey() {
		try {
			return controllerEngine.getCertificateManager().identityPrivateKey();
		} catch (Exception e) {
			return null;
		}
	}

	/** A production node meets a peer of another kind of exchange: one line per address (it retries every few seconds). */
	private void notPki(String remote, String what) {
		if (firstLegacyWarning("pki|" + remote)) {
			logger.error("Discovery: REFUSED " + what + " from " + remote + ": under security_profile=production discovery is"
					+ " authenticated by the operator's PKI only (no discovery secret is used or accepted); is that node production?");
		}
	}

	private String getDiscoverySecret(DiscoveryNode discoveryNode) {
		String discoverySecret = null;
		try {

			if (discoveryNode.discovery_type == DiscoveryType.AGENT) {

				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");

			} else if (discoveryNode.discovery_type == DiscoveryType.REGION) {

				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");

			} else if (discoveryNode.discovery_type == DiscoveryType.GLOBAL) {

				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");

			} else {
				logger.error("processDiscoveryNode UNKNOWN DISCOVERY TYPE");
			}

		} catch (Exception ex) {
			logger.error("getDiscoverySecret() " + ex.getMessage());
		}
		return discoverySecret;
	}

	// W-GFS-5: advertise the port THIS controller's broker is bound to (dynamic port included), so a
	// peer bridges to this broker and not to whatever happens to listen on discovery_port_remote on
	// this host. On one host with several regional brokers that was the global's broker: the bridge
	// came up against the wrong broker, the identity check failed, the path was declared lost and
	// re-discovered every ~7 s ("Controller Path Lost" flapping).
	private int advertisedBrokerPort() {
		try {
			if (controllerEngine.getBroker() != null) {
				return controllerEngine.getBroker().getBoundBrokerPort();
			}
		} catch (Exception ignore) { }
		return plugin.getConfig().getIntegerParam("broker_port", 32010);
	}

	public DiscoveryNode processIncomingBroadCastDiscovery(DiscoveryNode discoveryNode, String localAddress, int localPort, String remoteAddress, int remotePort) {
		try {

			if (discoveryNode.discovery_type == DiscoveryType.NETWORK) {
				if(discoveryNode.nodeType == DiscoveryNode.NodeType.BROADCAST) {
					discoveryNode.setBroadcastResponse(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(), plugin.getAgent());
				} else {
					logger.error("processIncomingDiscoveryNode() discoveryNode.nodeType != DiscoveryNode.NodeType.BROADCAST");
				}
			} else {
				if(controllerEngine.cstate.isRegionalController() && answersAs(discoveryNode.discovery_type) && pkiDiscovery()) {
					// production: a locator carries no secret and is answered with no secret (it only locates; the PKI exchange
					// that follows authenticates both sides); a secret-keyed locator or a legacy certify is not answered
					if (discoveryNode.nodeType == DiscoveryNode.NodeType.DISCOVER && DiscoveryAuth.PKI_LOCATOR.equals(discoveryNode.broadcast_validator)) {
						discoveryNode.setDiscovered(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(), plugin.getAgent(),
								controllerEngine.reachableAgentCount(), DiscoveryAuth.PKI_LOCATOR);
						discoveryNode.discovered_broker_port = advertisedBrokerPort();
					} else {
						notPki(remoteAddress, "a secret-keyed " + discoveryNode.discovery_type + " " + discoveryNode.nodeType + " locator");
						discoveryNode = null;
					}
				} else if(controllerEngine.cstate.isRegionalController() && answersAs(discoveryNode.discovery_type)) {

					String discoverySecret = getDiscoverySecret(discoveryNode);

					if(discoverySecret != null) {

						String decryptedString = discoveryCrypto.decrypt(discoveryNode.broadcast_validator, discoverySecret);
						if (decryptedString == null) legacyValidatorRefused(remoteAddress, discoveryNode.broadcast_validator);
						if (decryptedString != null) {
							if (decryptedString.equals(verifyMessage)) {

								if(discoveryNode.nodeType == DiscoveryNode.NodeType.DISCOVER) {
									// answered in the requester's format: an earlier build's (only inside the window, where decrypt accepted it), else v2
									String answer = DiscoveryCrypto.isLegacyFormat(discoveryNode.broadcast_validator)
											? discoveryCrypto.encryptLegacy(validateMessage, discoverySecret) : discoveryCrypto.encrypt(validateMessage, discoverySecret);
									discoveryNode.setDiscovered(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(),plugin.getAgent(),controllerEngine.reachableAgentCount(), answer);
									discoveryNode.discovered_broker_port = advertisedBrokerPort();
								} else if(discoveryNode.nodeType == DiscoveryNode.NodeType.CERTIFY && !legacyCertifyAllowed(remoteAddress, discoveryNode.discovery_type)) {
									discoveryNode = null;
								} else if(discoveryNode.nodeType == DiscoveryNode.NodeType.CERTIFY) {

									discoveryNode.legacy_exchange = true;
									discoveryNode.remote = remoteAddress;
									String discovered_cert = configureCertTrust(discoveryNode);
									if (discovered_cert == null) return null;     // refused (logged): no answer, nothing recorded
									discoveryNode.setCertified(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(),plugin.getAgent(),controllerEngine.reachableAgentCount(), discoveryCrypto.encryptLegacy(validateMessage,discoverySecret), discovered_cert);
									discoveryNode.discovered_broker_port = advertisedBrokerPort();

								} else {
									logger.error("processIncomingDiscoveryNode() discoveryNode.nodeType: " + discoveryNode.nodeType.name() + " !UNKNOWN!");
								}
							}
						}
					} else {
						logger.error("No secret found for " + discoveryNode.discovery_type.name() + " type");
						discoveryNode = null;
					}

				} else {
					logger.error("processDiscoveryNode() Discovery ERROR AGENT IN REGION SPACE");
					discoveryNode = null;
				}
			}

		} catch (Exception ex) {
			logger.error("processIncomingDiscoveryNode() " + ex.getMessage());
			discoveryNode = null;
		}
		return discoveryNode;
	}

	public DiscoveryNode generateBroadCastDiscovery(DiscoveryType disType, boolean sendCert) {
		return generateBroadCastDiscovery(disType, sendCert, sendCert);
	}

	/**
	 * A locator ({@code sendCert=false}) or the earlier builds' single-message certificate exchange ({@code sendCert=true},
	 * used only as the upgrade window's fallback to a responder that is not upgraded). {@code legacyFormat}: the validator in
	 * the earlier builds' (replayable) format -- only inside the upgrade window; otherwise always the v2 format, which every
	 * upgraded responder accepts with or without the window.
	 */
	public DiscoveryNode generateBroadCastDiscovery(DiscoveryType disType, boolean sendCert, boolean legacyFormat) {

		DiscoveryNode discoveryNode = null;

		try {

			discoveryNode = new DiscoveryNode(disType);

			if (disType == DiscoveryType.NETWORK) {
				return discoveryNode;
			}
			if (pkiDiscovery()) {
				if (sendCert) {
					logger.error("generateDiscovery(): the single-message certificate exchange is not used under security_profile=production");
					return null;
				}
				discoveryNode.setDiscover(DiscoveryAuth.PKI_LOCATOR);
				return discoveryNode;
			}
			if (disType != DiscoveryType.AGENT && disType != DiscoveryType.REGION && disType != DiscoveryType.GLOBAL) {
				logger.error("generateDiscovery() Discovery Type UNKNOWN");
				return discoveryNode;
			}
			String name = disType.name().toLowerCase(java.util.Locale.ROOT);
			String discoverySecret = getDiscoverySecret(discoveryNode);
			if (discoverySecret == null) {
				logger.error("discover() Error: discovery_secret_" + name + " = null, no " + name + " discovery");
				return discoveryNode;
			}
			String validator = legacyFormat ? discoveryCrypto.encryptLegacy(verifyMessage, discoverySecret)
					: discoveryCrypto.encrypt(verifyMessage, discoverySecret);
			if (validator == null) return null;
			if (sendCert) {
				ownLeafForDiscovery();
				discoveryNode.setCertify(validator, controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()), plugin.getRegion(), plugin.getAgent());
			} else {
				discoveryNode.setDiscover(validator);
			}
		} catch (Exception ex) {
			logger.error("generateDiscovery() Error: " + ex.getMessage());
		}
		return discoveryNode;
	}

	public boolean  isValidatedAuthenication(DiscoveryNode discoveryNode) {
		boolean isValidated = false;
		if (pkiDiscovery()) {
			// a locator answer: only a pointer (the PKI exchange that follows authenticates the responder)
			return discoveryNode != null && discoveryNode.nodeType == DiscoveryNode.NodeType.DISCOVERED
					&& DiscoveryAuth.PKI_LOCATOR.equals(discoveryNode.discovered_validator);
		}
		try {

			String discoverySecret = null;
			if (discoveryNode.discovery_type == DiscoveryType.AGENT) {
				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
			} else if (discoveryNode.discovery_type == DiscoveryType.REGION) {
				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
			} else if (discoveryNode.discovery_type == DiscoveryType.GLOBAL) {
				discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
			}

			if(discoverySecret != null) {
				String validatedString = discoveryCrypto.decrypt(discoveryNode.discovered_validator, discoverySecret);
				if (validatedString != null) {
					if (validatedString.equals(validateMessage)) {
						isValidated = true;
					} else {
						logger.error("isValidatedAuthenication() Error : validatedString != validateMessage");
					}
				} else {
					logger.error("isValidatedAuthenication() Error : validatedString == null ");
				}
			} else {
				logger.error("isValidatedAuthenication() Error : discoverySecret == null");
			}
		}
		catch(Exception ex) {
			logger.error(ex.getMessage());
		}

		return isValidated;
	}

	/** Make our leaf name our current identity before it goes out in a discovery exchange (legacy trust only). */
	private void ownLeafForDiscovery() {
		try {
			controllerEngine.getCertificateManager().ensureLeafIdentity();
		} catch (Exception ex) {
			logger.error("ownLeafForDiscovery " + ex.getMessage());
		}
	}

	/**
	 * Client side of a certified discovery exchange: trust the responder (or install the identity its region CA issued
	 * us), and record what binds later connections to it (PeerIdentity): in legacy per-node trust the responder's leaf
	 * key is pinned to the region/agent it reported; a REGION/GLOBAL responder is a controller allowed to bridge to us.
	 */
	public boolean setCertTrust(DiscoveryNode discoveryNode) {
		if (discoveryNode.remote == null) discoveryNode.remote = discoveryNode.discovered_ip;
		JUDGING.set(discoveryNode);
		try {
			return setCertTrustOf(discoveryNode);
		} finally {
			JUDGING.remove();
		}
	}

	/** The exchange this thread is judging: {@link #learnPeer} reads the peer's address from it and leaves why it refused it. */
	private static final ThreadLocal<DiscoveryNode> JUDGING = new ThreadLocal<>();

	private boolean setCertTrustOf(DiscoveryNode discoveryNode) {
		boolean regionalCa = plugin.getConfig().getBooleanParam("security_regional_ca", false);
		DiscoveryType type = discoveryNode.discovery_type;
		String region = discoveryNode.discovered_region, agent = discoveryNode.discovered_agent;
		if (controllerEngine.getCertificateManager().isProvisioned()) {
			// pre-provisioned: the responder must be a CONTROLLER of the operator's PKI naming itself as it answered (no pin is
			// learned: the PKI decides at every exchange and every handshake); nothing is installed
			io.cresco.agent.controller.communication.CertificateManager pcm = controllerEngine.getCertificateManager();
			String json = discoveryNode.discovered_server_cert != null ? discoveryNode.discovered_server_cert : discoveryNode.discovered_cert;
			java.security.cert.X509Certificate[] responder = json == null ? null : pcm.getCertsfromJson(json);
			String why = pcm.provisionedResponderRefusal(responder, type, region, agent);
			// a controller this node joined (its global, a peer region): recorded with its tenant -- its region is that tenant's here
			if (why == null && (type == DiscoveryType.REGION || type == DiscoveryType.GLOBAL)) why = pcm.provisionedControllerLearned(responder);
			if (why != null) {
				logger.error("Discovery REFUSED " + region + "/" + agent + ": " + why);
				return false;
			}
			return true;
		}
		// controller_trust_anchors: the RESPONDER's chain must have a validated path from its leaf to a listed anchor; under the
		// regional CA the identity it issued us must come from a CA on that path (a copied chain cannot hand out identities)
		io.cresco.agent.controller.communication.CertificateManager cm = controllerEngine.getCertificateManager();
		String serverJson = discoveryNode.discovered_server_cert != null ? discoveryNode.discovered_server_cert : discoveryNode.discovered_cert;
		String anchor = cm.controllerAnchorRefusal(serverJson == null ? null : cm.getCertsfromJson(serverJson),
				regionalCa && discoveryNode.discovered_server_cert != null && discoveryNode.discovered_cert != null
						? cm.getCertsfromJson(discoveryNode.discovered_cert) : null);
		if (anchor != null) {
			logger.error("Discovery REFUSED " + region + "/" + agent + ": " + anchor);
			return false;
		}
		// every check on the peer runs BEFORE anything it sent is recorded or trusted
		if (!learnPeer(type, region, agent, discoveryNode.discovered_cert, !regionalCa, true, false, discoveryNode.discovered_rebind)) return false;
		if (regionalCa) {
			io.cresco.agent.controller.communication.PeerTrustRegistry reg = controllerEngine.getCertificateManager().getPeerTrust();
			Object before = reg.caState(region);
			// what the responder's CA returned, validated and recorded for the responder's region only
			if (!controllerEngine.getCertificateManager().installEnrollment(discoveryNode.discovered_cert, region, type)) return false;
			if (!learnPeer(type, region, agent, discoveryNode.discovered_cert, false, true, true)) {
				reg.restoreCa(region, before);                      // nothing of a refused peer stays recorded
				return false;
			}
			return true;
		}
		// per-node trust: bind the responder's key first (it can be refused); only then trust its chain for TLS
		if (!learnPeer(type, region, agent, discoveryNode.discovered_cert, true, true, true, discoveryNode.discovered_rebind)) return false;
		// (taken without proof although it proved its key before: the certificates this node holds for it stay as they are)
		if (discoveryNode.unchanged) return true;
		return setCertTrust(discoveryNode.getDiscoveredPath(), discoveryNode.discovered_cert);
	}

	/**
	 * Record what an authenticated discovery exchange taught about the peer.
	 *
	 * <p>As the RESPONDER (the peer joined us): an AGENT joiner is bound first come, only inside this controller's region and
	 * tenant, never to a controller name or a superuser tenant ({@code agentJoinRefusal}); a REGION/GLOBAL joiner is bound
	 * first come as a controller (reserved) and becomes a bridge peer -- unless its certificate names a superuser tenant:
	 * then only when the operator listed it ({@code controllerJoinRefusal}). The discovery secret proves membership, not a
	 * name or a tenant.
	 *
	 * <p>As the CLIENT (we joined the peer): the responder is our controller. An AGENT responder must be in the region and
	 * tenant we joined; a REGION/GLOBAL responder must not claim our region, nor a superuser tenant unless the operator
	 * listed it. Its key is bound as a controller, strictly: another key for that name later needs the operator (a rebind
	 * line, a signed rebind statement) or the CA this node pinned for the region.
	 *
	 * <p>Under per-node trust everything recorded is decided and written by one call,
	 * {@link io.cresco.agent.controller.communication.PeerTrustRegistry#join}: nothing new without proof of the peer's key
	 * ({@link DiscoveryNode#proved}), one controller per region, one region per key, one global above this node.
	 */
	private boolean learnPeer(DiscoveryType type, String region, String agent, String certJson, boolean pinLeaf, boolean asClient) {
		return learnPeer(type, region, agent, certJson, pinLeaf, asClient, true);
	}

	/** As above; with {@code record=false} only the checks run (nothing is bound or allowed). */
	private boolean learnPeer(DiscoveryType type, String region, String agent, String certJson, boolean pinLeaf, boolean asClient,
							  boolean record) {
		return learnPeer(type, region, agent, certJson, pinLeaf, asClient, record, null);
	}

	/** As above; {@code rebindStatement}: what the responder served (an operator-signed controller rebind statement), or null. */
	private boolean learnPeer(DiscoveryType type, String region, String agent, String certJson, boolean pinLeaf, boolean asClient,
							  boolean record, String rebindStatement) {
		return learnPeer(type, region, agent, certJson, pinLeaf, asClient, record, rebindStatement, false);
	}

	/**
	 * As above; {@code legacyExchange}: the responder side of the legacy single-message exchange (the upgrade window), whose
	 * binding is recorded as {@link io.cresco.agent.controller.communication.PeerTrustRegistry#SOURCE_LEGACY}. Such a binding
	 * is the one an authenticated (v2) exchange of the same agent name may replace, once, while the window is open: the
	 * node was upgraded, and an upgraded build has a new key (an earlier build kept none).
	 */
	private boolean learnPeer(DiscoveryType type, String region, String agent, String certJson, boolean pinLeaf, boolean asClient,
							  boolean record, String rebindStatement, boolean legacyExchange) {
		try {
			if (!io.cresco.agent.controller.communication.CrescoNames.isValid(region)
					|| !io.cresco.agent.controller.communication.CrescoNames.isValid(agent)) {
				logger.warn("Discovery: not learning a peer with an invalid identity name " + region + "/" + agent);
				return false;
			}
			io.cresco.agent.controller.communication.PeerTrustRegistry reg = controllerEngine.getCertificateManager().getPeerTrust();
			boolean controllerExchange = type == DiscoveryType.REGION || type == DiscoveryType.GLOBAL;
			java.security.cert.X509Certificate[] certs = certJson == null ? null : controllerEngine.getCertificateManager().getCertsfromJson(certJson);
			io.cresco.library.security.CrescoIdentity named = (certs == null || certs.length == 0) ? null
					: io.cresco.agent.controller.communication.CrescoNames.identityOf(certs[0]);
			String myRegion = controllerEngine.cstate.getRegion();
			String myTenant = io.cresco.agent.controller.communication.SecuritySettings.tenant(plugin.getConfig());
			if (asClient) {
				// (controller_trust_anchors are checked on the responder's chain by setCertTrust before this)
				if (!controllerExchange && !region.equals(myRegion)) {
					logger.error("Discovery REFUSED " + region + "/" + agent + ": it answered as the controller of region " + region
							+ " but this node joined region " + myRegion);
					return false;
				}
				if (!controllerExchange && pinLeaf && named != null && !myTenant.equals(named.getTenant())) {
					logger.error("Discovery REFUSED " + region + "/" + agent + ": its tenant " + named.getTenant() + " is not this node's " + myTenant);
					return false;
				}
				if (controllerExchange && region.equals(myRegion)) {
					logger.error("Discovery REFUSED " + region + "/" + agent + ": a controller of this node's own region answered a "
							+ type + " exchange");
					return false;
				}
			} else if (!controllerExchange && pinLeaf) {
				String refusal = controllerEngine.getCertificateManager().agentJoinRefusal(named == null ? null : named.getTenant(), region, agent);
				if (refusal != null) {
					logger.error("Discovery REFUSED agent " + region + "/" + agent + ": " + refusal);
					return false;
				}
			} else if (controllerExchange && region.equals(myRegion)) {
				logger.error("Discovery REFUSED " + region + "/" + agent + ": a joining controller claims this node's region");
				return false;
			}
			if (controllerExchange && pinLeaf) {
				// a controller that joins, or that this node joins: its tenant is its own certificate's word, so one in a
				// superuser tenant is bound only when the operator listed its key (never first come)
				String refusal = controllerEngine.getCertificateManager().controllerJoinRefusal(certs);
				if (refusal != null) {
					logger.error("Discovery REFUSED controller " + region + "/" + agent + ": " + refusal);
					return false;
				}
			}
			DiscoveryNode at = JUDGING.get();
			// The chain the peer presented as ITSELF. Per-node trust: the one it is bound by. Under the regional CA (pinLeaf false)
			// certJson is, for this node as the joiner, what the responder's CA RETURNED -- the responder's own chain is the one of
			// its CHALLENGE; for this node as the responder it is the joiner's chain, as before.
			java.security.cert.X509Certificate[] itsChain = pinLeaf || !asClient ? certs
					: at == null || at.discovered_server_cert == null ? null
					: controllerEngine.getCertificateManager().getCertsfromJson(at.discovered_server_cert);
			boolean unchanged = false;
			if (itsChain != null && itsChain.length > 0 && itsChain[0] != null) {
				// Everything this exchange may record of the peer is decided, and recorded, by ONE call under the registry's lock
				// (PeerTrustRegistry.join): the name's own rules, the proof of possession, one controller per region, one global
				// above this node. With record=false only the checks run. Under the regional CA the same call decides -- before
				// the CA issues or records anything -- and what the CA then records is its own, as before.
				io.cresco.agent.controller.communication.CertificateManager cm = controllerEngine.getCertificateManager();
				String keyFp = io.cresco.agent.controller.communication.PeerIdentity.spkiFingerprint(itsChain[0]);
				io.cresco.library.security.CrescoIdentity its = io.cresco.agent.controller.communication.CrescoNames.identityOf(itsChain[0]);
				io.cresco.agent.controller.communication.PeerTrustRegistry.Join j = new io.cresco.agent.controller.communication.PeerTrustRegistry.Join();
				j.region = region;
				j.agent = agent;
				j.chain = itsChain;
				j.role = (asClient || controllerExchange) ? io.cresco.agent.controller.communication.PeerTrustRegistry.ROLE_CONTROLLER
						: io.cresco.agent.controller.communication.PeerTrustRegistry.ROLE_AGENT;
				j.source = legacyExchange ? io.cresco.agent.controller.communication.PeerTrustRegistry.SOURCE_LEGACY : "discovery";
				j.proved = at != null && at.proved;
				j.window = controllerEngine.legacyNodesAdmitted();
				j.client = asClient;
				j.global = asClient && type == DiscoveryType.GLOBAL;
				j.legacyUpgrade = legacyUpgrade(type, asClient, legacyExchange, region, agent);
				if (asClient && rebindStatement != null) {
					String statementWhy = cm.rebindStatementRefusal(rebindStatement, region, agent, keyFp);
					if (statementWhy == null) j.statement = "an operator-signed rebind statement";
					else if (record) logger.warn("Discovery: the rebind statement " + region + "/" + agent + " served was not accepted: " + statementWhy);
				}
				j.anchored = (asClient || controllerExchange) && cm.controllerAnchorVouches(itsChain);
				j.scopedAnchor = (asClient || controllerExchange) ? cm.controllerAnchorScoped(itsChain) : null;
				j.recordedAnchored = cm::recordedControllerAnchored;
				j.graceMs = Math.max(0L, plugin.getConfig().getLongParam("discovery_proof_grace_days", 7L)) * 86_400_000L;
				j.peerAddress = at == null ? null : at.remote;
				j.exchange = type.name();
				j.record = record;
				io.cresco.agent.controller.communication.PeerTrustRegistry.BindResult res = reg.join(j);
				if (!res.ok) {
					// the refused peer is always told what ITS operator can do, whatever this node's log has room for
					if (at != null) at.refusal = res.toPeer;
					if (res.limited == null || reg.refusalLogged(res.limited, region, agent, keyFp)) {
						logger.error("Discovery REFUSED " + (j.role.equals(io.cresco.agent.controller.communication.PeerTrustRegistry.ROLE_AGENT) ? "agent " : "controller ")
								+ region + "/" + agent + (its == null ? "" : " (tenant " + its.getTenant() + ", key " + keyFp + ")") + " at "
								+ (at == null || at.remote == null ? "an unknown address" : at.remote)
								+ (asClient ? ", which answered a " + type + " exchange" : ", which joined in a " + type + " exchange") + ": " + res.reason
								+ (res.limited == null ? "" : " (refused " + reg.refusedTimes(res.limited, region, agent, keyFp)
								+ " time(s) so far; said again at most every 10 minutes)"));
					}
					return false;
				}
				// taken without proof although it proved its key here before (the grace, the window): nothing recorded changes
				unchanged = res.unchanged;
				if (at != null) at.unchanged = unchanged;
				if (res.warn != null) logger.warn("Discovery: " + res.warn);
			}
			if (!record || unchanged) return true;
			if (controllerExchange) {
				reg.allowBridgePeer(region, agent);
			}
			return true;
		} catch (Exception ex) {
			logger.error("learnPeer " + ex.getMessage());
			return false;
		}
	}

	public boolean setCertTrust(String remoteAgentPath, String remoteCertString) {
		boolean isSet = false;
		try {
			if (plugin.getConfig().getBooleanParam("security_regional_ca", false)) {
				// Regional-CA (Option C): the response carries THIS node's region-signed identity —
				// install it (swap our cert chain, trust the issuing CA that rides in the chain).
				isSet = controllerEngine.getCertificateManager().installEnrollment(remoteCertString);
			} else {
				Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
				controllerEngine.getCertificateManager().replaceTrustStoreEntries(remoteAgentPath, certs);
				isSet = true;
			}
		} catch(Exception ex) {
			logger.error("setCertTrust Error " + ex.getMessage());
		}
		return isSet;
	}

	/** Responder side of a certified discovery exchange (see {@link #setCertTrust(DiscoveryNode)}). */
	private String configureCertTrust(DiscoveryNode discoveryNode) {
		JUDGING.set(discoveryNode);
		try {
			return configureCertTrustOf(discoveryNode);
		} finally {
			JUDGING.remove();
		}
	}

	private String configureCertTrustOf(DiscoveryNode discoveryNode) {
		io.cresco.agent.controller.communication.CertificateManager cm = controllerEngine.getCertificateManager();
		if (cm.isProvisioned()) {
			// pre-provisioned: the joiner's identity is its certificate from the operator's PKI -- nothing is bound or issued
			java.security.cert.X509Certificate[] joiner = cm.getCertsfromJson(discoveryNode.discover_cert);
			String why = cm.provisionedJoinerRefusal(joiner, discoveryNode.discovery_type, discoveryNode.discover_region, discoveryNode.discover_agent);
			// a controller that joined this node: recorded with its tenant -- its region is that tenant's here from now on
			if (why == null && (discoveryNode.discovery_type == DiscoveryType.REGION || discoveryNode.discovery_type == DiscoveryType.GLOBAL)) {
				why = cm.provisionedControllerLearned(joiner);
			}
			if (why != null) {
				logger.error("Discovery REFUSED " + discoveryNode.discover_region + "/" + discoveryNode.discover_agent + ": " + why);
				return null;
			}
			return cm.getJsonFromCerts(cm.getPublicCertificate());
		}
		boolean regionalCa = plugin.getConfig().getBooleanParam("security_regional_ca", false);
		if (!regionalCa) {
			// per-node trust: refuse before trusting anything when the joiner may not have this name
			java.security.cert.X509Certificate[] c = controllerEngine.getCertificateManager().getCertsfromJson(discoveryNode.discover_cert);
			io.cresco.library.security.CrescoIdentity id = (c == null || c.length == 0) ? null
					: io.cresco.agent.controller.communication.CrescoNames.identityOf(c[0]);
			String scope = id == null ? null : discoveryNode.discovery_type == DiscoveryType.AGENT
					? controllerEngine.getCertificateManager().agentJoinRefusal(id.getTenant(), discoveryNode.discover_region, discoveryNode.discover_agent)
					: controllerEngine.getCertificateManager().controllerJoinRefusal(c);
			if (scope != null) {
				logger.error("Discovery REFUSED " + (discoveryNode.discovery_type == DiscoveryType.AGENT ? "agent " : "controller ")
						+ discoveryNode.discover_region + "/" + discoveryNode.discover_agent + ": " + scope);
				return null;
			}
			io.cresco.agent.controller.communication.PeerTrustRegistry.BindResult free = (id == null) ? null
					: controllerEngine.getCertificateManager().getPeerTrust().canBind(id.getTenant(), discoveryNode.discover_region, discoveryNode.discover_agent, c[0].getPublicKey());
			if (free != null && !free.ok && (legacyUpgrade(discoveryNode.discovery_type, false, discoveryNode.legacy_exchange,
					discoveryNode.discover_region, discoveryNode.discover_agent)
					|| (discoveryNode.discovery_type != DiscoveryType.AGENT && controllerEngine.getCertificateManager().controllerAnchorScoped(c) != null))) {
				// learnPeer replaces the legacy binding, or the key of a controller the operator's scoped anchor states (or refuses)
				free = null;
			} else if (free == null || !free.ok) {
				logger.error("Discovery REFUSED " + discoveryNode.discover_region + "/" + discoveryNode.discover_agent + ": "
						+ (free == null ? "its certificate names no valid identity" : free.reason));
				return null;
			}
		}
		if (!regionalCa) {
			// bind the joiner's key first (it can still be refused); only then trust its chain for TLS
			if (!learnPeer(discoveryNode.discovery_type, discoveryNode.discover_region, discoveryNode.discover_agent,
					discoveryNode.discover_cert, true, false, true, null, discoveryNode.legacy_exchange)) {
				return null;
			}
			// (taken without proof although it proved its key before: the certificates this node holds for it stay as they are)
			if (discoveryNode.unchanged) {
				return controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
			}
			return configureCertTrust(discoveryNode.getDiscoverPath(), discoveryNode.discover_cert, discoveryNode.discovery_type);
		}
		// regional CA: issueEnrollment checks and records the joiner itself; the peer is then recorded as a bridge
		if (!learnPeer(discoveryNode.discovery_type, discoveryNode.discover_region, discoveryNode.discover_agent,
				discoveryNode.discover_cert, false, false, false)) {
			return null;
		}
		String local = configureCertTrust(discoveryNode.getDiscoverPath(), discoveryNode.discover_cert, discoveryNode.discovery_type);
		if (local != null && !learnPeer(discoveryNode.discovery_type, discoveryNode.discover_region, discoveryNode.discover_agent,
				discoveryNode.discover_cert, false, false)) {
			return null;
		}
		return local;
	}

	/**
	 * May an authenticated exchange of AGENT {@code region/agent} (this node the responder) replace the binding an earlier
	 * build of that agent made over the legacy exchange? Only inside the upgrade window, only for an agent joiner, never
	 * from a legacy exchange itself.
	 */
	private boolean legacyUpgrade(DiscoveryType type, boolean asClient, boolean legacyExchange, String region, String agent) {
		return !asClient && !legacyExchange && type == DiscoveryType.AGENT && controllerEngine.legacyNodesAdmitted()
				&& controllerEngine.getCertificateManager().getPeerTrust().isLegacyBinding(region, agent);
	}

	private String configureCertTrust(String remoteAgentPath, String remoteCertString, DiscoveryType joinType) {
		String localCertString = null;
		try {
			if (plugin.getConfig().getBooleanParam("security_regional_ca", false)) {
				// Regional-CA (Option C): act as the issuing authority — sign the joiner a leaf under our
				// region CA and return the chain for it to install. We trust the joiner via our own CA
				// (no per-node truststore entry needed), so trust material stays O(regions), not O(nodes^2).
				// (an agent's enrollment adds no trust anchor -- its key is bound in the registry, which TLS consults live -- so the
				// broker's TLS context is not rewritten at every (re-)enrollment; a federating controller's CA is added, and the
				// context refreshed, by federateController)
				localCertString = controllerEngine.getCertificateManager().issueEnrollment(remoteCertString, joinType);
			} else {
				Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
				// the peer's current chain REPLACES its previous one (a renewed chain is not one more anchor). The broker's
				// trust reads the truststore's index at each handshake (LivePkixTrustManager): the chain is trusted from the
				// moment this returns, and the broker's TLS context is not rebuilt (it was, from the whole store, at every join)
				controllerEngine.getCertificateManager().replaceTrustStoreEntries(remoteAgentPath, certs);
				localCertString = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
			}
		} catch(Exception ex) {
			logger.error("configureCertTrust Error " + ex.getMessage());
		}
		return localCertString;
	}

	// ------------------------------------------------------------------ discovery v2 (DiscoveryAuth)

	public DiscoveryAuth.Mode authMode() {
		return DiscoveryAuth.Mode.of(plugin.getConfig());
	}

	/** Per controller (a processor is made per connection): the addresses already warned about. */
	private static final java.util.Map<ControllerEngine, java.util.Set<String>> WARNED =
			java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());
	private final java.util.Set<String> warnedLegacy;
	static final int MAX_WARNED_LEGACY = 1024;

	/**
	 * Whether to log a legacy-exchange line for {@code key} (a peer address, which UDP lets anyone spoof): once per key
	 * for up to {@link #MAX_WARNED_LEGACY} keys, then at most one line per 10 s, so the set never grows past the cap.
	 */
	boolean firstLegacyWarning(String key) {
		if (warnedLegacy.contains(key)) return false;
		if (warnedLegacy.size() < MAX_WARNED_LEGACY) return warnedLegacy.add(key);
		return DiscoveryGuard.logAllowed("discovery-legacy-overflow");
	}

	int warnedLegacyCount() { return warnedLegacy.size(); }

	/** The legacy single-message certificate exchange: only with discovery_auth=compat (a rolling upgrade). */
	private boolean legacyCertifyAllowed(String remoteAddress, DiscoveryType type) {
		if (type != DiscoveryType.AGENT) {
			// the legacy token is the constant DISCOVER token anyone on the LAN may have seen: it never makes a bridge peer
			if (firstLegacyWarning(remoteAddress + "|" + type)) {
				logger.error("Discovery: REFUSED a legacy " + type + " certificate exchange from " + remoteAddress
						+ " (even with discovery_auth=compat, only agent membership may use the legacy exchange; upgrade that controller)");
			}
			return false;
		}
		if (authMode() == DiscoveryAuth.Mode.COMPAT) {
			if (firstLegacyWarning("accept|" + remoteAddress)) {
				logger.warn("Discovery: accepting the LEGACY unauthenticated certificate exchange from " + remoteAddress
						+ " (discovery_auth=compat; upgrade that node, then remove compat)");
			}
			return true;
		}
		if (firstLegacyWarning("refuse|" + remoteAddress)) {
			logger.error("Discovery: REFUSED a legacy (replayable, unauthenticated) certificate exchange from " + remoteAddress
					+ "; upgrade that node, or open the upgrade window on this one (discovery_auth=compat with discovery_compat_until)");
		}
		return false;
	}

	/**
	 * A locator whose validator is not the authenticated format (an earlier build's, which is replayable) failed to decrypt
	 * because the upgrade window is not open here: say so once per address (an earlier build retries every few seconds).
	 */
	private void legacyValidatorRefused(String remoteAddress, String validator) {
		if (DiscoveryAuth.PKI_LOCATOR.equals(validator)) {
			if (firstLegacyWarning("pki-locator|" + remoteAddress)) {
				logger.error("Discovery: not answering the PKI locator of a security_profile=production node at " + remoteAddress
						+ ": this node is of the standard profile (the two never complete a discovery exchange)");
			}
			return;
		}
		if (validator == null || validator.startsWith("v2:") || authMode() == DiscoveryAuth.Mode.COMPAT) return;
		if (firstLegacyWarning("refuse|" + remoteAddress)) {
			logger.error("Discovery: REFUSED a node of an earlier build at " + remoteAddress + " (legacy, replayable discovery"
					+ " validator): upgrade that node, or open the upgrade window on this one (discovery_auth=compat with"
					+ " discovery_compat_until)");
		}
	}

	private String secretFor(String type) {
		try {
			DiscoveryNode n = new DiscoveryNode(DiscoveryType.valueOf(type));
			return getDiscoverySecret(n);
		} catch (Exception e) {
			return null;
		}
	}

	private java.security.cert.X509Certificate leafOf(String chainJson) {
		java.security.cert.X509Certificate[] c = chainJson == null ? null : controllerEngine.getCertificateManager().getCertsfromJson(chainJson);
		return (c == null || c.length == 0) ? null : c[0];
	}

	/** Client: the HELLO that opens a v2 exchange of {@code type}, or null when this node has no secret for it. */
	public DiscoveryAuth.Msg clientHello(DiscoveryType type) {
		boolean pki = pkiDiscovery();
		String secret = pki ? null : secretFor(type.name());
		if (!pki && secret == null) {
			logger.error("discovery v2: no discovery secret for " + type);
			return null;
		}
		if (plugin.getConfig().getBooleanParam("security_regional_ca", false) && type != DiscoveryType.AGENT
				&& controllerEngine.cstate.isRegionalController()) {
			controllerEngine.getCertificateManager().ensureRegionCA();
		}
		ownLeafForDiscovery();
		DiscoveryAuth.Msg h = new DiscoveryAuth.Msg();
		h.step = "hello";
		h.type = type.name();
		// the secret's exchange: this build proves possession of its key, and says so in its nonce (DiscoveryAuth.PROOF_NONCE)
		h.nc = pki ? DiscoveryAuth.nonce() : DiscoveryAuth.proofNonce();
		h.creg = plugin.getRegion();
		h.cagt = plugin.getAgent();
		h.ccert = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
		if (pki) h.auth = DiscoveryAuth.AUTH_PKI;
		return h;
	}

	/** Client: check the responder's CHALLENGE; the PROOF to send, or null (logged) when it does not authenticate. */
	public DiscoveryAuth.Msg clientProof(DiscoveryAuth.Msg hello, DiscoveryAuth.Msg ch, String remote) {
		if (ch == null || !"challenge".equals(ch.step) || !hello.nc.equals(ch.nc) || !hello.type.equals(ch.type)) {
			logger.error("discovery v2: " + remote + " sent no valid challenge");
			return null;
		}
		java.security.cert.X509Certificate sleaf = leafOf(ch.scert);
		if (!DiscoveryAuth.leafNames(sleaf, ch.sreg, ch.sagt)) {
			logger.error("discovery v2: " + remote + " claims " + ch.sreg + "/" + ch.sagt + " but its certificate does not name it");
			return null;
		}
		if (pkiDiscovery()) return clientProofPki(hello, ch, remote);
		byte[] key = DiscoveryAuth.key(secretFor(hello.type), "handshake|" + hello.type);
		byte[] t = DiscoveryAuth.transcript(hello, ch, leafOf(hello.ccert), leafOf(ch.scert), false);
		if (!DiscoveryAuth.macEquals(ch.mac, DiscoveryAuth.mac(key, "S1", t))) {
			logger.error("discovery v2: " + remote + " (claiming " + ch.sreg + "/" + ch.sagt + ") failed authentication: it does not hold the "
					+ hello.type + " discovery secret, or the exchange was altered");
			return null;
		}
		DiscoveryAuth.Msg p = new DiscoveryAuth.Msg();
		p.step = "proof";
		p.type = hello.type;
		p.mac = DiscoveryAuth.mac(key, "C", DiscoveryAuth.transcript(hello, ch, leafOf(hello.ccert), leafOf(ch.scert), true));
		// proof of possession, next to the MAC: this exchange signed with the key of the leaf this node presented
		java.security.PrivateKey own = ownKey();
		if (own == null) {
			logger.error("discovery v2: this node's identity key is not readable: it cannot prove its key to " + remote);
			return null;
		}
		p.pop = DiscoveryAuth.proofSign(own, DiscoveryAuth.POP_CLIENT, DiscoveryAuth.possessionTranscript(hello, ch, leafOf(hello.ccert), sleaf));
		if (p.pop == null) {
			logger.error("discovery v2: this node's identity key is of type " + own.getAlgorithm() + ": the proof of its key is defined for RSA"
					+ " and EC keys only, so it cannot join a node of this build (give it an RSA or EC identity)");
			return null;
		}
		return p;
	}

	/**
	 * Client, PKI exchange: the responder's whole chain verifies to the trust anchors as the controller it answered as, and
	 * its S1 signature (over both nonces, both identities, its chain and port) verifies under its leaf key; then sign C.
	 */
	private DiscoveryAuth.Msg clientProofPki(DiscoveryAuth.Msg hello, DiscoveryAuth.Msg ch, String remote) {
		if (!DiscoveryAuth.AUTH_PKI.equals(ch.auth)) {
			notPki(remote, "a secret-keyed answer (" + ch.sreg + "/" + ch.sagt + ")");
			return null;
		}
		java.security.cert.X509Certificate[] schain = chainOf(ch.scert);
		String why = (schain == null || schain.length == 0) ? "no certificate chain"
				: controllerEngine.getCertificateManager().provisionedResponderRefusal(schain, DiscoveryType.valueOf(hello.type), ch.sreg, ch.sagt);
		if (why != null) {
			logger.error("discovery (PKI): " + remote + " answering as " + ch.sreg + "/" + ch.sagt + " is REFUSED: " + why);
			return null;
		}
		byte[] t = DiscoveryAuth.transcript(hello, ch, leafOf(hello.ccert), schain[0], false);
		if (!DiscoveryAuth.verify(schain[0].getPublicKey(), ch.mac, "pki-S1", t)) {
			logger.error("discovery (PKI): " + remote + " (claiming " + ch.sreg + "/" + ch.sagt + ") failed authentication: its signature"
					+ " does not verify under its certificate (a replay, an altered exchange, or not the key's holder)");
			return null;
		}
		java.security.PrivateKey own = ownKey();
		if (own == null) {
			logger.error("discovery (PKI): this node's identity key is not readable");
			return null;
		}
		DiscoveryAuth.Msg p = new DiscoveryAuth.Msg();
		p.step = "proof";
		p.type = hello.type;
		p.auth = DiscoveryAuth.AUTH_PKI;
		p.mac = DiscoveryAuth.sign(own, "pki-C", DiscoveryAuth.transcript(hello, ch, leafOf(hello.ccert), schain[0], true));
		return p;
	}

	/**
	 * Client: check the RESULT and, when it authenticates and grants, record/install what it carries. Returns the
	 * certified responder (its authenticated identity and broker port) or null (logged).
	 */
	public DiscoveryNode clientResult(DiscoveryAuth.Msg hello, DiscoveryAuth.Msg ch, DiscoveryAuth.Msg r, String remoteAddress, int discoveryPort) {
		if (r == null || !"result".equals(r.step)) {
			logger.error("discovery v2: " + remoteAddress + " sent no result");
			return null;
		}
		byte[] t = DiscoveryAuth.transcript(hello, ch, leafOf(hello.ccert), leafOf(ch.scert), true);
		boolean authentic = pkiDiscovery()
				? DiscoveryAuth.AUTH_PKI.equals(r.auth) && DiscoveryAuth.verify(leafOf(ch.scert) == null ? null : leafOf(ch.scert).getPublicKey(),
						r.mac, "pki-S2", t, DiscoveryAuth.resultDigest(r))
				: DiscoveryAuth.macEquals(r.mac, DiscoveryAuth.mac(DiscoveryAuth.key(secretFor(hello.type), "handshake|" + hello.type), "S2", t,
						DiscoveryAuth.resultDigest(r)));
		if (!authentic) {
			logger.error("discovery v2: the result from " + remoteAddress + " failed authentication (altered or replayed)");
			return null;
		}
		// proof of possession: a responder whose nonce says it is a build that proves its key signed this result with the key
		// of the leaf it presented -- or it is refused, whatever its MACs say (no downgrade). One whose nonce does not say so
		// is a build of before the proof (release 1.3.378), or says it is: the registry gives it only what such a build has.
		boolean proved = false;
		if (!pkiDiscovery()) {
			java.security.cert.X509Certificate sleaf = leafOf(ch.scert);
			proved = sleaf != null && DiscoveryAuth.proofVerify(sleaf.getPublicKey(), r.pop, DiscoveryAuth.POP_RESULT,
					DiscoveryAuth.possessionTranscript(hello, ch, leafOf(hello.ccert), sleaf), DiscoveryAuth.resultDigest(r));
			if (!proved && DiscoveryAuth.claimsProof(ch.ns) && sleaf != null && DiscoveryAuth.proofAlgorithm(sleaf.getPublicKey().getAlgorithm()) == null) {
				logger.error("discovery v2: " + ch.sreg + "/" + ch.sagt + " at " + remoteAddress + " presents a certificate with a key of type "
						+ sleaf.getPublicKey().getAlgorithm() + ": the proof of a key is defined for RSA and EC keys only: REFUSED, nothing recorded");
				return null;
			}
			if (!proved && DiscoveryAuth.claimsProof(ch.ns)) {
				logger.error("discovery v2: " + remoteAddress + " answered as " + ch.sreg + "/" + ch.sagt + " and holds the " + hello.type
						+ " discovery secret, but did NOT prove the key of the certificate it presented (no signature by that key, or one"
						+ " made for another exchange): REFUSED, nothing recorded. It is not " + ch.sreg + "/" + ch.sagt
						+ " -- a holder of the secret with a copy of that certificate -- or the exchange was altered");
				return null;
			}
		}
		if (!r.ok) {
			logger.error("discovery v2: " + ch.sreg + "/" + ch.sagt + " at " + remoteAddress + " REFUSED this node: " + r.reason);
			return null;
		}
		DiscoveryNode n = new DiscoveryNode(DiscoveryType.valueOf(hello.type));
		n.proved = proved;
		n.nodeType = DiscoveryNode.NodeType.CERTIFIED;
		n.discovered_ip = remoteAddress;
		n.discovered_port = discoveryPort;
		n.discovered_region = ch.sreg;
		n.discovered_agent = ch.sagt;
		n.discovered_broker_port = ch.sport;
		n.discovered_cert = plugin.getConfig().getBooleanParam("security_regional_ca", false) ? r.rcert : ch.scert;
		n.discovered_rebind = ch.rebind;
		n.discovered_server_cert = ch.scert;
		n.discover_region = hello.creg;
		n.discover_agent = hello.cagt;
		if (!setCertTrust(n)) {
			logger.error("discovery v2: could not record " + ch.sreg + "/" + ch.sagt + " (see above)");
			return null;
		}
		logger.info("discovery " + (pkiDiscovery() ? "(PKI)" : "v2") + ": authenticated " + hello.type + " exchange with " + ch.sreg + "/" + ch.sagt + " at " + remoteAddress);
		return n;
	}

	/** Responder: the CHALLENGE for a HELLO, or a refusal RESULT. */
	public DiscoveryAuth.Msg serverChallenge(DiscoveryAuth.Msg h, String remote) {
		DiscoveryAuth.Msg ch = new DiscoveryAuth.Msg();
		ch.step = "challenge";
		boolean pki = pkiDiscovery();
		if (h != null && pki != DiscoveryAuth.AUTH_PKI.equals(h.auth)) {
			if (pki) notPki(remote, "a secret-keyed hello (" + h.creg + "/" + h.cagt + ")");
			else logger.warn("discovery v2: not answering a PKI hello from " + remote + " (" + h.creg + "/" + h.cagt + "): this node is"
					+ " not security_profile=production");
			return null;
		}
		String secret = (h == null || h.type == null || pki) ? null : secretFor(h.type);
		if (h == null || !"hello".equals(h.step) || (secret == null && !pki) || h.type == null || h.nc == null
				|| !controllerEngine.cstate.isRegionalController()) {
			logger.warn("discovery v2: not answering a hello from " + remote + " (unknown type, no secret for it, or not a controller)");
			return null;
		}
		DiscoveryType ht;
		try { ht = DiscoveryType.valueOf(h.type); } catch (Exception e) { return null; }
		if (!answersAs(ht)) {
			logger.warn("discovery v2: not answering a GLOBAL hello from " + remote + ": this node is not the fabric's global controller by the operator's PKI");
			return null;
		}
		if (!DiscoveryAuth.leafNames(leafOf(h.ccert), h.creg, h.cagt)) {
			logger.warn("discovery v2: hello from " + remote + " claims " + h.creg + "/" + h.cagt + " but its certificate does not name it");
			return null;
		}
		if (pki) {
			// the joiner's whole chain verifies to the trust anchors as what it claims BEFORE this node signs anything for it
			java.security.cert.X509Certificate[] cchain = chainOf(h.ccert);
			String why = (cchain == null || cchain.length == 0) ? "no certificate chain"
					: controllerEngine.getCertificateManager().provisionedJoinerRefusal(cchain, ht, h.creg, h.cagt);
			if (why != null) {
				logger.error("discovery (PKI): REFUSED the hello of " + h.creg + "/" + h.cagt + " from " + remote + ": " + why);
				return null;
			}
		}
		if (plugin.getConfig().getBooleanParam("security_regional_ca", false)) controllerEngine.getCertificateManager().ensureRegionCA();
		controllerEngine.getCertificateManager().bindOwnIdentity();      // no joiner is ever bound to this node's own name
		ch.type = h.type;
		ch.nc = h.nc;
		ch.ns = pki ? DiscoveryAuth.nonce() : DiscoveryAuth.proofNonce();
		ch.sreg = plugin.getRegion();
		ch.sagt = plugin.getAgent();
		ch.scert = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
		ch.rebind = controllerEngine.getCertificateManager().controllerRebindStatement();
		ch.sport = advertisedBrokerPort();
		if (pki) {
			java.security.PrivateKey own = ownKey();
			if (own == null) {
				logger.error("discovery (PKI): this node's identity key is not readable");
				return null;
			}
			ch.auth = DiscoveryAuth.AUTH_PKI;
			ch.mac = DiscoveryAuth.sign(own, "pki-S1", DiscoveryAuth.transcript(h, ch, leafOf(h.ccert), leafOf(ch.scert), false));
			return ch;
		}
		byte[] key = DiscoveryAuth.key(secret, "handshake|" + h.type);
		ch.mac = DiscoveryAuth.mac(key, "S1", DiscoveryAuth.transcript(h, ch, leafOf(h.ccert), leafOf(ch.scert), false));
		return ch;
	}

	/** Responder: check the PROOF; only then record the client and issue/return certificates. Always a RESULT. */
	public DiscoveryAuth.Msg serverResult(DiscoveryAuth.Msg h, DiscoveryAuth.Msg ch, DiscoveryAuth.Msg p, String remote) {
		DiscoveryAuth.Msg r = new DiscoveryAuth.Msg();
		r.step = "result";
		r.type = h.type;
		boolean pki = pkiDiscovery();
		byte[] key = pki ? null : DiscoveryAuth.key(secretFor(h.type), "handshake|" + h.type);
		// the client's PROOF covers the whole chain it sent: a chain altered on the way fails here, before anything is recorded
		byte[] t = DiscoveryAuth.transcript(h, ch, leafOf(h.ccert), leafOf(ch.scert), true);
		java.security.cert.X509Certificate cleaf = leafOf(h.ccert);
		boolean authentic = p != null && "proof".equals(p.step) && (pki
				? DiscoveryAuth.AUTH_PKI.equals(p.auth) && cleaf != null && DiscoveryAuth.verify(cleaf.getPublicKey(), p.mac, "pki-C", t)
				: DiscoveryAuth.macEquals(p.mac, DiscoveryAuth.mac(key, "C", t)));
		// proof of possession (the secret's exchange): the client's signature over this exchange by the key of the leaf it sent
		byte[] pt = pki || !authentic ? null : DiscoveryAuth.possessionTranscript(h, ch, cleaf, leafOf(ch.scert));
		boolean proved = pt != null && cleaf != null && DiscoveryAuth.proofVerify(cleaf.getPublicKey(), p.pop, DiscoveryAuth.POP_CLIENT, pt);
		if (!authentic) {
			logger.warn("discovery " + (pki ? "(PKI)" : "v2") + ": " + remote + " (claiming " + h.creg + "/" + h.cagt + ") failed"
					+ " authentication: " + (pki ? "its signature does not verify under its certificate" : "wrong " + h.type + " secret")
					+ ", a replay, or an altered exchange; nothing recorded");
			r.ok = false;
			r.reason = "authentication failed";
		} else if (!pki && !proved && DiscoveryAuth.claimsProof(h.nc) && cleaf != null
				&& DiscoveryAuth.proofAlgorithm(cleaf.getPublicKey().getAlgorithm()) == null) {
			logger.error("discovery v2: " + remote + " presented for " + h.creg + "/" + h.cagt + " a certificate with a key of type "
					+ cleaf.getPublicKey().getAlgorithm() + ": the proof of a key is defined for RSA and EC keys only: REFUSED, nothing recorded");
			r.ok = false;
			r.reason = "refused by " + plugin.getRegion() + "/" + plugin.getAgent() + ": the key of " + h.creg + "/" + h.cagt + " is of type "
					+ cleaf.getPublicKey().getAlgorithm() + ", and the proof of a key is defined for RSA and EC keys only";
		} else if (!pki && !proved && DiscoveryAuth.claimsProof(h.nc)) {
			// its nonce says it is a build that proves its key, and it did not: never taken as an earlier build instead
			logger.error("discovery v2: " + remote + " presented the certificate of " + h.creg + "/" + h.cagt + " and holds the " + h.type
					+ " discovery secret, but did NOT prove that certificate's key (no signature by it, or one made for another exchange):"
					+ " REFUSED, nothing recorded. It is not " + h.creg + "/" + h.cagt + " -- a holder of the secret with a copy of that"
					+ " certificate -- or the exchange was altered");
			r.ok = false;
			r.reason = "refused by " + plugin.getRegion() + "/" + plugin.getAgent() + ": no proof of the key of the certificate presented for "
					+ h.creg + "/" + h.cagt;
		} else {
			DiscoveryNode n = new DiscoveryNode(DiscoveryType.valueOf(h.type));
			n.discover_region = h.creg;
			n.discover_agent = h.cagt;
			n.discover_cert = h.ccert;
			n.remote = remote;
			n.proved = proved;
			String local = configureCertTrust(n);
			r.ok = local != null;
			r.reason = r.ok ? null : "refused by " + plugin.getRegion() + "/" + plugin.getAgent() + (n.refusal != null ? ": " + n.refusal
					: " (identity taken, invalid, or outside this CA's scope; see its log)");
			r.rcert = local;
			if (r.ok) logger.info("discovery " + (pki ? "(PKI)" : "v2") + ": authenticated " + h.type + " exchange with " + h.creg + "/" + h.cagt + " at " + remote);
		}
		if (pki) {
			java.security.PrivateKey own = ownKey();
			r.auth = DiscoveryAuth.AUTH_PKI;
			r.mac = own == null ? null : DiscoveryAuth.sign(own, "pki-S2", t, DiscoveryAuth.resultDigest(r));
			return r;
		}
		r.mac = DiscoveryAuth.mac(key, "S2", t, DiscoveryAuth.resultDigest(r));
		if (authentic) {
			// this node's own proof: the result (granted or refused) signed with the key of the leaf it presented. Only for a
			// client whose MAC verified: a stranger cannot make this node sign.
			java.security.PrivateKey own = ownKey();
			if (own == null) logger.error("discovery v2: this node's identity key is not readable: it cannot prove its key to " + remote);
			else r.pop = DiscoveryAuth.proofSign(own, DiscoveryAuth.POP_RESULT,
					DiscoveryAuth.possessionTranscript(h, ch, cleaf, leafOf(ch.scert)), DiscoveryAuth.resultDigest(r));
		}
		return r;
	}
}
