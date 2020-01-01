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
								} else if(discoveryNode.nodeType == DiscoveryNode.NodeType.CERTIFY) {

									String discovered_cert = configureCertTrust(discoveryNode.getDiscoverPath(), discoveryNode.discover_cert);
									discoveryNode.setCertified(localAddress, localPort, remoteAddress, remotePort, plugin.getRegion(),plugin.getAgent(),controllerEngine.reachableAgents().size(), discoveryCrypto.encrypt(validateMessage,discoverySecret), discovered_cert);

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

	public boolean setCertTrust(String remoteAgentPath, String remoteCertString) {
		boolean isSet = false;
		try {
			Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
			controllerEngine.getCertificateManager().addCertificatesToTrustStore(remoteAgentPath,certs);
			isSet = true;

		} catch(Exception ex) {
			logger.error("configureCertTrust Error " + ex.getMessage());
		}
		return isSet;
	}

	private String configureCertTrust(String remoteAgentPath, String remoteCertString) {
		String localCertString = null;
		try {
			Certificate[] certs = controllerEngine.getCertificateManager().getCertsfromJson(remoteCertString);
			controllerEngine.getCertificateManager().addCertificatesToTrustStore(remoteAgentPath,certs);
			controllerEngine.getBroker().updateTrustManager();
			localCertString = controllerEngine.getCertificateManager().getJsonFromCerts(controllerEngine.getCertificateManager().getPublicCertificate());
		} catch(Exception ex) {
			logger.error("configureCertTrust Error " + ex.getMessage());
		}
		return localCertString;
	}
}
