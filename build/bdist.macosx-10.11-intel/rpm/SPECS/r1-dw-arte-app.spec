%define name r1-dw-arte-app
%define version 0.0.1
%define unmangled_version 0.0.1
%define release 1

Summary: ARTE app
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Basanth Roy, Chu Chi Liu <broy@radiumone.com;cliu@radiumone.com>
Url: ssh://git@git.dev.dw.sc.gwallet.com:7999/scm/dw/r1-dw-arte-app.git

%description
UNKNOWN

%prep
%setup -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
