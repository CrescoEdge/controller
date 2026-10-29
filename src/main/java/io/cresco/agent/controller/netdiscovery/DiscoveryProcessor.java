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
				if(controllerEngine.cstate.isRegionalController()) {

					String discoverySecret = getDiscoverySecret(discoveryNode);

					if(discoverySecret != null) {

						String decryptedString = discoveryCrypto.decrypt(discoveryNode.broadcast_validator, discoverySecret);
						if (decryptedString != null) {
							if (decryptedString.equals(verifyMessage)) {

								if(discoveryNode.nodeType == DiscoveryNode.NodeType.DISCOVER) {
									discoveryNode.setDiscovered(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(),plugin.getAgent(),controllerEngine.reachableAgents().size(), discoveryCrypto.encrypt(validateMessage,discoverySecret));
									discoveryNode.discovered_broker_port = advertisedBrokerPort();
								} else if(discoveryNode.nodeType == DiscoveryNode.NodeType.CERTIFY && !legacyCertifyAllowed(remoteAddress, discoveryNode.discovery_type)) {
									discoveryNode = null;
								} else if(discoveryNode.nodeType == DiscoveryNode.NodeType.CERTIFY) {

									String discovered_cert = configureCertTrust(discoveryNode);
									discoveryNode.setCertified(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(),plugin.getAgent(),controllerEngine.reachableAgents().size(), discoveryCrypto.encrypt(validateMessage,discoverySecret), discovered_cert);
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

		DiscoveryNode discoveryNode = null;

		try {

			discoveryNode = new DiscoveryNode(disType);

			if (disType == DiscoveryType.AGENT) {

				String discoverySecret = plugin.getConfig().getStringParam("discovery_secret_agent");
				if (discoverySecret != null) {
					if(sendCert) {
						ownLeafForDiscovery();
						//sme.setParam("public_cert", controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()));

						discoveryNode.setCertify(discoveryCrypto.encrypt(verifyMessage, discoverySecret), controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()), plugin.getRegion(), plugin.getAgent());
					} else {
						discoveryNode.setDiscover(discoveryCrypto.encrypt(verifyMessage, discoverySecret));
					}
				} else {
					logger.error("discover() Error: discovery_secret_agent = null, no agent discovery");
				}

			} else if (disType == DiscoveryType.REGION) {

				String discoverySecret = plugin.getConfig().getStringParam("discovery_secret_region");
				if (discoverySecret != null) {
					if(sendCert) {
						ownLeafForDiscovery();
						discoveryNode.setCertify(discoveryCrypto.encrypt(verifyMessage, discoverySecret),controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()), plugin.getRegion(), plugin.getAgent());
					} else {
						discoveryNode.setDiscover(discoveryCrypto.encrypt(verifyMessage, discoverySecret));
					}
				} else {
					logger.error("discover() Error: discovery_secret_region = null, no region discovery");
				}

			} else if (disType == DiscoveryType.GLOBAL) {
				String discoverySecret = plugin.getConfig().getStringParam("discovery_secret_global");
				if (discoverySecret != null) {
					if(sendCert) {
						ownLeafForDiscovery();
						discoveryNode.setCertify(discoveryCrypto.encrypt(verifyMessage, discoverySecret),controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate()), plugin.getRegion(), plugin.getAgent());
					} else {
						discoveryNode.setDiscover(discoveryCrypto.encrypt(verifyMessage, discoverySecret));
					}
				} else {
					logger.error("discover() Error: discovery_secret_global = null, , no global discovery");
				}

			} else if (disType == DiscoveryType.NETWORK) {

				discoveryNode = new DiscoveryNode(disType);

			} else {
				logger.error("generateDiscovery() Discovery Type UNKNOWN");
			}
		} catch (Exception ex) {
			logger.error("generateDiscovery() Error: " + ex.getMessage());
		}
		return discoveryNode;
	}

	public boolean  isValidatedAuthenication(DiscoveryNode discoveryNode) {
		boolean isValidated = false;
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
		boolean isSet;
		if (plugin.getConfig().getBooleanParam("security_regional_ca", false)) {
			// what the responder's CA returned, recorded for the responder's region only
			isSet = controllerEngine.getCertificateManager().installEnrollment(discoveryNode.discovered_cert, discoveryNode.discovered_region,
					discoveryNode.discovery_type);
		} else {
			isSet = setCertTrust(discoveryNode.getDiscoveredPath(), discoveryNode.discovered_cert);
		}
		if (isSet) {
			isSet = learnPeer(discoveryNode.discovery_type, discoveryNode.discovered_region, discoveryNode.discovered_agent,
					discoveryNode.discovered_cert, !plugin.getConfig().getBooleanParam("security_regional_ca", false), true);
		}
		return isSet;
	}

	/**
	 * Record what an authenticated discovery exchange taught about the peer.
	 *
	 * <p>As the RESPONDER (the peer joined us): an AGENT joiner is bound first come, only inside this controller's region and
	 * tenant, never to a controller name or a superuser tenant ({@code agentJoinRefusal}); a REGION/GLOBAL joiner is bound
	 * first come as a controller (reserved) and becomes a bridge peer. The discovery secret proves membership, not a name.
	 *
	 * <p>As the CLIENT (we joined the peer): the responder is our controller. An AGENT responder must be in the region and
	 * tenant we joined; a REGION/GLOBAL responder must not claim our region. Its key is bound as a controller and FOLLOWS
	 * authenticated discovery: a re-installed controller is accepted (logged), because a joiner cannot tell a re-install from
	 * anything else and refusing would cut every agent off until each was reconfigured.
	 */
	private boolean learnPeer(DiscoveryType type, String region, String agent, String certJson, boolean pinLeaf, boolean asClient) {
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
			String myTenant = plugin.getConfig().getStringParam("tenant_id", "default");
			if (asClient) {
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
			if (pinLeaf && certs != null && certs.length > 0 && certs[0] != null) {
				String role = (asClient || controllerExchange) ? io.cresco.agent.controller.communication.PeerTrustRegistry.ROLE_CONTROLLER
						: io.cresco.agent.controller.communication.PeerTrustRegistry.ROLE_AGENT;
				String why = reg.pin(region, agent, certs[0], role, asClient);
				if (why != null) {
					logger.error("Discovery REFUSED " + region + "/" + agent + ": " + why);
					return false;
				}
			}
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
				controllerEngine.getCertificateManager().addCertificatesToTrustStore(remoteAgentPath, certs);
				isSet = true;
			}
		} catch(Exception ex) {
			logger.error("setCertTrust Error " + ex.getMessage());
		}
		return isSet;
	}

	/** Responder side of a certified discovery exchange (see {@link #setCertTrust(DiscoveryNode)}). */
	private String configureCertTrust(DiscoveryNode discoveryNode) {
		boolean regionalCa = plugin.getConfig().getBooleanParam("security_regional_ca", false);
		if (!regionalCa) {
			// per-node trust: refuse before trusting anything when the joiner may not have this name
			java.security.cert.X509Certificate[] c = controllerEngine.getCertificateManager().getCertsfromJson(discoveryNode.discover_cert);
			io.cresco.library.security.CrescoIdentity id = (c == null || c.length == 0) ? null
					: io.cresco.agent.controller.communication.CrescoNames.identityOf(c[0]);
			String scope = (id == null || discoveryNode.discovery_type != DiscoveryType.AGENT) ? null
					: controllerEngine.getCertificateManager().agentJoinRefusal(id.getTenant(), discoveryNode.discover_region, discoveryNode.discover_agent);
			if (scope != null) {
				logger.error("Discovery REFUSED agent " + discoveryNode.discover_region + "/" + discoveryNode.discover_agent + ": " + scope);
				return null;
			}
			io.cresco.agent.controller.communication.PeerTrustRegistry.BindResult free = (id == null) ? null
					: controllerEngine.getCertificateManager().getPeerTrust().canBind(id.getTenant(), discoveryNode.discover_region, discoveryNode.discover_agent, c[0].getPublicKey());
			if (free == null || !free.ok) {
				logger.error("Discovery REFUSED " + discoveryNode.discover_region + "/" + discoveryNode.discover_agent + ": "
						+ (free == null ? "its certificate names no valid identity" : free.reason));
				return null;
			}
		}
		String local = configureCertTrust(discoveryNode.getDiscoverPath(), discoveryNode.discover_cert, discoveryNode.discovery_type);
		if (local != null && !learnPeer(discoveryNode.discovery_type, discoveryNode.discover_region, discoveryNode.discover_agent,
				discoveryNode.discover_cert, !regionalCa, false)) {
			return null;
		}
		return local;
	}

	private String configureCertTrust(String remoteAgentPath, String remoteCertString, DiscoveryType joinType) {
		String localCertString = null;
		try {
			if (plugin.getConfig().getBooleanParam("security_regional_ca", false)) {
				// Regional-CA (Option C): act as the issuing authority — sign the joiner a leaf under our
				// region CA and return the chain for it to install. We trust the joiner via our own CA
				// (no per-node truststore entry needed), so trust material stays O(regions), not O(nodes^2).
				localCertString = controllerEngine.getCertificateManager().issueEnrollment(remoteCertString, joinType);
				if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager();
			} else {
				Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
				controllerEngine.getCertificateManager().addCertificatesToTrustStore(remoteAgentPath, certs);
				if (controllerEngine.getBroker() != null) controllerEngine.getBroker().updateTrustManager();
				localCertString = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
			}
		} catch(Exception ex) {
			logger.error("configureCertTrust Error " + ex.getMessage());
		}
		return localCertString;
	}

	// ------------------------------------------------------------------ discovery v2 (DiscoveryAuth)

	public DiscoveryAuth.Mode authMode() {
		return DiscoveryAuth.Mode.parse(plugin.getConfig().getStringParam(DiscoveryAuth.MODE_KEY, "v2"));
	}

	private final java.util.Set<String> warnedLegacy = java.util.concurrent.ConcurrentHashMap.newKeySet();
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
			if (firstLegacyWarning(String.valueOf(remoteAddress))) {
				logger.warn("Discovery: accepting the LEGACY unauthenticated certificate exchange from " + remoteAddress
						+ " (discovery_auth=compat; upgrade that node, then remove compat)");
			}
			return true;
		}
		if (firstLegacyWarning(String.valueOf(remoteAddress))) {
			logger.error("Discovery: REFUSED a legacy (replayable, unauthenticated) certificate exchange from " + remoteAddress
					+ "; upgrade that node, or set discovery_auth=compat on this one during the upgrade");
		}
		return false;
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
		java.security.cert.X509Certificate[] c = controllerEngine.getCertificateManager().getCertsfromJson(chainJson);
		return (c == null || c.length == 0) ? null : c[0];
	}

	/** Client: the HELLO that opens a v2 exchange of {@code type}, or null when this node has no secret for it. */
	public DiscoveryAuth.Msg clientHello(DiscoveryType type) {
		String secret = secretFor(type.name());
		if (secret == null) {
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
		h.nc = DiscoveryAuth.nonce();
		h.creg = plugin.getRegion();
		h.cagt = plugin.getAgent();
		h.ccert = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
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
		byte[] key = DiscoveryAuth.key(secretFor(hello.type), "handshake|" + hello.type);
		byte[] t = DiscoveryAuth.transcript(hello, ch, leafOf(hello.ccert), sleaf);
		if (!DiscoveryAuth.macEquals(ch.mac, DiscoveryAuth.mac(key, "S1", t))) {
			logger.error("discovery v2: " + remote + " (claiming " + ch.sreg + "/" + ch.sagt + ") failed authentication: it does not hold the "
					+ hello.type + " discovery secret, or the exchange was altered");
			return null;
		}
		DiscoveryAuth.Msg p = new DiscoveryAuth.Msg();
		p.step = "proof";
		p.type = hello.type;
		p.mac = DiscoveryAuth.mac(key, "C", t);
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
		byte[] key = DiscoveryAuth.key(secretFor(hello.type), "handshake|" + hello.type);
		byte[] t = DiscoveryAuth.transcript(hello, ch, leafOf(hello.ccert), leafOf(ch.scert));
		if (!DiscoveryAuth.macEquals(r.mac, DiscoveryAuth.mac(key, "S2", t, DiscoveryAuth.resultDigest(r)))) {
			logger.error("discovery v2: the result from " + remoteAddress + " failed authentication (altered or replayed)");
			return null;
		}
		if (!r.ok) {
			logger.error("discovery v2: " + ch.sreg + "/" + ch.sagt + " at " + remoteAddress + " REFUSED this node: " + r.reason);
			return null;
		}
		DiscoveryNode n = new DiscoveryNode(DiscoveryType.valueOf(hello.type));
		n.nodeType = DiscoveryNode.NodeType.CERTIFIED;
		n.discovered_ip = remoteAddress;
		n.discovered_port = discoveryPort;
		n.discovered_region = ch.sreg;
		n.discovered_agent = ch.sagt;
		n.discovered_broker_port = ch.sport;
		n.discovered_cert = plugin.getConfig().getBooleanParam("security_regional_ca", false) ? r.rcert : ch.scert;
		n.discover_region = hello.creg;
		n.discover_agent = hello.cagt;
		if (!setCertTrust(n)) {
			logger.error("discovery v2: could not record " + ch.sreg + "/" + ch.sagt + " (see above)");
			return null;
		}
		logger.info("discovery v2: authenticated " + hello.type + " exchange with " + ch.sreg + "/" + ch.sagt + " at " + remoteAddress);
		return n;
	}

	/** Responder: the CHALLENGE for a HELLO, or a refusal RESULT. */
	public DiscoveryAuth.Msg serverChallenge(DiscoveryAuth.Msg h, String remote) {
		DiscoveryAuth.Msg ch = new DiscoveryAuth.Msg();
		ch.step = "challenge";
		String secret = (h == null || h.type == null) ? null : secretFor(h.type);
		if (h == null || !"hello".equals(h.step) || secret == null || h.nc == null || !controllerEngine.cstate.isRegionalController()) {
			logger.warn("discovery v2: not answering a hello from " + remote + " (unknown type, no secret for it, or not a controller)");
			return null;
		}
		if (!DiscoveryAuth.leafNames(leafOf(h.ccert), h.creg, h.cagt)) {
			logger.warn("discovery v2: hello from " + remote + " claims " + h.creg + "/" + h.cagt + " but its certificate does not name it");
			return null;
		}
		if (plugin.getConfig().getBooleanParam("security_regional_ca", false)) controllerEngine.getCertificateManager().ensureRegionCA();
		controllerEngine.getCertificateManager().bindOwnIdentity();      // no joiner is ever bound to this node's own name
		ch.type = h.type;
		ch.nc = h.nc;
		ch.ns = DiscoveryAuth.nonce();
		ch.sreg = plugin.getRegion();
		ch.sagt = plugin.getAgent();
		ch.scert = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
		ch.sport = advertisedBrokerPort();
		byte[] key = DiscoveryAuth.key(secret, "handshake|" + h.type);
		ch.mac = DiscoveryAuth.mac(key, "S1", DiscoveryAuth.transcript(h, ch, leafOf(h.ccert), leafOf(ch.scert)));
		return ch;
	}

	/** Responder: check the PROOF; only then record the client and issue/return certificates. Always a RESULT. */
	public DiscoveryAuth.Msg serverResult(DiscoveryAuth.Msg h, DiscoveryAuth.Msg ch, DiscoveryAuth.Msg p, String remote) {
		DiscoveryAuth.Msg r = new DiscoveryAuth.Msg();
		r.step = "result";
		r.type = h.type;
		byte[] key = DiscoveryAuth.key(secretFor(h.type), "handshake|" + h.type);
		byte[] t = DiscoveryAuth.transcript(h, ch, leafOf(h.ccert), leafOf(ch.scert));
		if (p == null || !"proof".equals(p.step) || !DiscoveryAuth.macEquals(p.mac, DiscoveryAuth.mac(key, "C", t))) {
			logger.warn("discovery v2: " + remote + " (claiming " + h.creg + "/" + h.cagt + ") failed authentication: wrong "
					+ h.type + " secret, a replay, or an altered exchange; nothing recorded");
			r.ok = false;
			r.reason = "authentication failed";
		} else {
			DiscoveryNode n = new DiscoveryNode(DiscoveryType.valueOf(h.type));
			n.discover_region = h.creg;
			n.discover_agent = h.cagt;
			n.discover_cert = h.ccert;
			String local = configureCertTrust(n);
			r.ok = local != null;
			r.reason = r.ok ? null : "refused by " + plugin.getRegion() + "/" + plugin.getAgent() + " (identity taken, invalid, or outside this CA's scope; see its log)";
			r.rcert = local;
			if (r.ok) logger.info("discovery v2: authenticated " + h.type + " exchange with " + h.creg + "/" + h.cagt + " at " + remote);
		}
		r.mac = DiscoveryAuth.mac(key, "S2", t, DiscoveryAuth.resultDigest(r));
		return r;
	}
}
